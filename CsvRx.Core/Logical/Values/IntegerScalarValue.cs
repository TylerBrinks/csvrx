using CsvRx.Core.Data;

namespace CsvRx.Core.Logical.Values;

internal record IntegerScalarValue(long Value) : ScalarValue(Value, ColumnDataType.Integer)
{
    public override string ToString()
    {
        return $"INT64({Value})";
    }
}