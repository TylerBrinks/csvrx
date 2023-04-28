using CsvRx.Core.Data;

namespace CsvRx.Core.Logical.Values;

internal record FloatScalarValue(float Value) : ScalarValue(Value, ColumnDataType.Decimal)
{
    public override string ToString()
    {
        return $"FLOAT({Value})";
    }
}