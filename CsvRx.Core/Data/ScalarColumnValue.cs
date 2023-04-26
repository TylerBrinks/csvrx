using CsvRx.Core.Logical.Expressions;
using CsvRx.Data;

namespace CsvRx.Core.Data;

internal record ScalarColumnValue(ScalarValue Value, int RecordCount, ColumnDataType DataType) : ColumnValue(DataType)
{
    internal override int Size => RecordCount;

    internal override object GetValue(int i)
    {
        return Value.RawValue;
    }
}