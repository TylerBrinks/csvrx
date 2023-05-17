using System.Text.RegularExpressions;

namespace CsvRx.Core.Data;

public partial class InferredDataType
{
    private static readonly List<Regex> TypeExpressions = new()
    {
        BooleanRegex(),
        IntegerRegex(),
        DoubleRegex(),
        Date32Regex(),
        TimestampSecond(),
        TimestampMillisecond(),
        TimestampMicrosecond(),
        TimestampNanosecond()
    };

    public void Update(string value, Regex? datetimeRegex = null)
    {
        if (value.StartsWith("\""))
        {
            DataType |= ColumnDataType.Utf8;
            return;
        }

        var matched = false;
        for (var i = 0; i < TypeExpressions.Count; i++)
        {
            if (!TypeExpressions[i].IsMatch(value)) { continue; }

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
    private static partial Regex DoubleRegex();

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