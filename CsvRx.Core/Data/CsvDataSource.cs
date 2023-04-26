﻿using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using CsvRx.Core.Physical.Execution;
using CsvRx.Data;
using CsvRx.Physical;

namespace CsvRx.Core.Data;

internal class CsvDataSource : DataSource
{
    private readonly string _filePath;
    private readonly CsvOptions _options;
    private Schema _schema;

    public CsvDataSource(string filePath, CsvOptions options)
    {
        _filePath = filePath;
        _options = options;
        Schema = InferSchema();
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
        var columnTypes = new List<Core.Data.InferredDataType>(Enumerable.Range(0, headers.Length).Select(_ => new Core.Data.InferredDataType()));
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

    internal IEnumerable<List<string?[]>> Read(List<int> indices)
    {
        using var reader = new StreamReader(_filePath);
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = _options.Delimiter,
            HasHeaderRecord = _options.HasHeader,
        };
        using var csv = new CsvReader(reader, config);

        csv.Read();
        csv.ReadHeader();

        var lines = new List<string?[]>();
        var count = 0;

        while (csv.Read())
        {
            var line = new string?[indices.Count];

            for (var j = 0; j < indices.Count; j++)
            {
                var value = csv.GetField(indices[j]);
                line[j] = value;
            }
            
            lines.Add(line);

            if (++count == 3)
            {
                count = 0;
                var slice = lines.Select(_=>_).ToList();
                lines.Clear();
                yield return slice;
            }
        }

        yield return lines;
    }

    public override Schema Schema { get; }

    public override IExecutionPlan Scan(List<int> projection)
    {
        return new CsvExec(_schema, projection, this);
    }
}