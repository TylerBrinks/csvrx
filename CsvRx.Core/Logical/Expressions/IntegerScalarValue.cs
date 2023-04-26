using CsvRx.Data;

namespace CsvRx.Core.Logical.Expressions;

internal record IntegerScalarValue(long Value) : ScalarValue(Value, ColumnDataType.Integer)
{
    public override string ToString()
    {
        return $"INT64({Value})";
    }
}