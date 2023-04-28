using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using CsvRx.Core.Physical;
using CsvRx.Core.Physical.Execution;

namespace CsvRx.Core.Data;

internal class CsvDataSource : DataSource
{
    private readonly string _filePath;
    private readonly CsvOptions _options;
    private Schema? _schema;

    public CsvDataSource(string filePath, CsvOptions options)
    {
        _filePath = filePath;
        _options = options;
        _schema = InferSchema();
    }

    private Schema InferSchema()
    {
        if (_schema != null)
        {
            return _schema;
        }

        using var reader = new StreamReader(_filePath);
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = _options.Delimiter,
            HasHeaderRecord = _options.HasHeader,
        };
        using var csv = new CsvReader(reader, config);

        csv.Read();
        csv.ReadHeader();

        var headers = csv.HeaderRecord;
        var columnTypes = new List<InferredDataType>(Enumerable.Range(0, headers.Length).Select(_ => new InferredDataType()));
        var rowCount = 0;

        while (csv.Read())
        {
            for (var i = 0; i < headers.Length; i++)
            {
                var value = csv.GetField(i);
                if (value == null) { continue; }
                columnTypes[i].Update(value);
            }

            rowCount++;

            if (rowCount == _options.InferMax)
            {
                break;
            }
        }

        var fields = headers.Select((h, i) => new Field(h, columnTypes[i].DataType)).ToList();

        _schema = new Schema(fields);
        return _schema;
    }

    internal async IAsyncEnumerable<List<string?[]>> Read(List<int> indices)
    {
        using var reader = new StreamReader(_filePath);
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = _options.Delimiter,
            HasHeaderRecord = _options.HasHeader,
        };
        using var csv = new CsvReader(reader, config);

        await csv.ReadAsync();
        csv.ReadHeader();

        var lines = new List<string?[]>();
        var count = 0;

        while (await csv.ReadAsync())
        {
            var line = new string?[indices.Count];

            for (var j = 0; j < indices.Count; j++)
            {
                var value = csv.GetField(indices[j]);
                line[j] = value;
            }
            
            lines.Add(line);

            if (++count != _options.ReadBatchSize)
            {
                continue;
            }

            count = 0;
            var slice = lines.Select(_=>_).ToList();
            lines.Clear();

            yield return slice;
        }

        yield return lines;
    }

    public override IExecutionPlan Scan(List<int> projection)
    {
        return new CsvExecution(_schema!, projection, this);
    }

    public override Schema? Schema => _schema;
}