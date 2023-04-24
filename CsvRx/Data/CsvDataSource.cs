using System.Globalization;
using System.Text.RegularExpressions;
using CsvHelper;
using CsvHelper.Configuration;
using CsvRx.Physical;

namespace CsvRx.Data;

public class CsvDataSource : DataSource
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

    public override Schema Schema { get; }

    public override IExecutionPlan Scan(List<int> projection)
    {
        return new CsvExec(_schema, projection);
    }
}

public partial class InferredDataType
{
    private static readonly List<Regex> TypeExpressions = new()
    {
        BooleanRegex(),
        IntegerRegex(),
        DecimalRegex(),
        Date32Regex(),
        TimestampSecond(),
        TimestampMillisecond(),
        TimestampMicrosecond(),
        TimestampNanosecond()
    };

    internal void Update(string value, Regex? datetimeRegex = null)
    {
        if (value.StartsWith("\""))
        {
            DataType |= ColumnDataType.Utf8;
            return;
        }

        var matched = false;
        for (var i = 0; i < TypeExpressions.Count; i++)
        {
            if (!TypeExpressions[i].IsMatch(value)){ continue; }

            DataType |= (ColumnDataType)(1 << i);
            matched = true;
        }

        if (matched) { return; }

        DataType = datetimeRegex != null && datetimeRegex.IsMatch(value)
            ? ColumnDataType.TimestampNanosecond
            : ColumnDataType.Utf8;
    }

    public override string ToString()
    {
        return DataType.ToString();
    }

    public ColumnDataType DataType { get; private set; }

    #region Generated expressions
    [GeneratedRegex("(?i)^(true)$|^(false)$(?-i)", RegexOptions.None, "en-US")]
    private static partial Regex BooleanRegex();
    [GeneratedRegex("^-?(\\d+)$", RegexOptions.None, "en-US")]
    private static partial Regex IntegerRegex();
    [GeneratedRegex("^-?((\\d*\\.\\d+|\\d+\\.\\d*)([eE]-?\\d+)?|\\d+([eE]-?\\d+))$", RegexOptions.None, "en-US")]
    private static partial Regex DecimalRegex();
    [GeneratedRegex("^\\d{4}-\\d\\d-\\d\\d$", RegexOptions.None, "en-US")]
    private static partial Regex Date32Regex();
    [GeneratedRegex("^\\d{4}-\\d\\d-\\d\\d[T ]\\d\\d:\\d\\d:\\d\\d$", RegexOptions.None, "en-US")]
    private static partial Regex TimestampSecond();
    [GeneratedRegex("^\\d{4}-\\d\\d-\\d\\d[T ]\\d\\d:\\d\\d:\\d\\d.\\d{1,3}$", RegexOptions.None, "en-US")]
    private static partial Regex TimestampMillisecond();
    [GeneratedRegex("^\\d{4}-\\d\\d-\\d\\d[T ]\\d\\d:\\d\\d:\\d\\d.\\d{1,6}$", RegexOptions.None, "en-US")]
    private static partial Regex TimestampMicrosecond();
    [GeneratedRegex("^\\d{4}-\\d\\d-\\d\\d[T ]\\d\\d:\\d\\d:\\d\\d.\\d{1,9}$", RegexOptions.None, "en-US")]
    private static partial Regex TimestampNanosecond();
    #endregion
}

public class CsvExec : IExecutionPlan
{
    private readonly List<int> _projection;

    public CsvExec(Schema schema, List<int> projection)
    {
        var fields = schema.Fields.Where((_, i) => projection.Contains(i)).ToList();
        Schema = new Schema(fields);
        _projection = projection;
    }
    // hasHeader
    //delimiter

    public Schema Schema { get; }
}