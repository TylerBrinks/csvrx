using CsvRx.Core.Data;

namespace CsvRx.Core.Logical.Values;

internal record FloatScalar(float Value) : ScalarValue(Value, ColumnDataType.Decimal)
{
    public override string ToString()
    {
        return $"FLOAT({Value})";
    }
}