using CsvRx.Core.Data;

namespace CsvRx.Core.Values;

internal record BooleanColumnValue(bool[] Values) : ColumnValue(ColumnDataType.Boolean)
{
    internal override int Size => Values.Length;

    internal override object GetValue(int index)
    {
        return Values[index];
    }
}