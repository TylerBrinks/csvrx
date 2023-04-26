using CsvRx.Data;

namespace CsvRx.Core.Logical.Expressions;

internal record BooleanScalarValue(bool Value) : ScalarValue(Value, ColumnDataType.Boolean)
{
    public override string ToString()
    {
        return Value.ToString();
    }
}