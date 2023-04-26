using CsvRx.Data;

namespace CsvRx.Core.Logical.Expressions;

internal record FloatScalarValue(float Value) : ScalarValue(Value, ColumnDataType.Decimal)
{
    public override string ToString()
    {
        return $"FLOAT({Value})";
    }
}