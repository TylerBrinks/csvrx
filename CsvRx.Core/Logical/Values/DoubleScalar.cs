using CsvRx.Core.Data;

namespace CsvRx.Core.Logical.Values;

internal record DoubleScalar(double Value) : ScalarValue(Value, ColumnDataType.Double)
{
    public override string ToString()
    {
        return $"DOUBLE({Value})";
    }
}