using CsvRx.Core.Data;
using CsvRx.Core.Logical.Values;

namespace CsvRx.Core.Values;

internal record ScalarColumnValue(ScalarValue Value, int RecordCount, ColumnDataType DataType) : ColumnValue(DataType)
{
    internal override int Size => RecordCount;

    internal override object? GetValue(int i)
    {
        return Value.RawValue;
    }
}