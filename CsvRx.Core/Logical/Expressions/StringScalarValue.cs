using CsvRx.Data;

namespace CsvRx.Core.Logical.Expressions;

internal record StringScalarValue(string Value) : ScalarValue(Value, ColumnDataType.Utf8)
{
    public override string ToString()
    {
        return Value;
    }
}