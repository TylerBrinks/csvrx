using CsvRx.Core.Data;

namespace CsvRx.Core.Logical.Values;

internal record StringScalarValue(string Value) : ScalarValue(Value, ColumnDataType.Utf8)
{
    public override string ToString()
    {
        return Value;
    }
}