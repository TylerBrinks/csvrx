using CsvRx.Core.Data;

namespace CsvRx.Core.Logical.Values;

internal record IntegerScalar(long Value) : ScalarValue(Value, ColumnDataType.Integer)
{
    public override string ToString()
    {
        return $"INT64({Value})";
    }
}