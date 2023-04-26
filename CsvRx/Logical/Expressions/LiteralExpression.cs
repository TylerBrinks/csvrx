using CsvRx.Data;

namespace CsvRx.Logical.Expressions;

internal record LiteralExpression(ScalarValue Value) : ILogicalExpression
{
    public override string ToString()
    {
        return Value.ToString();
    }
}

public abstract record ScalarValue(object? RawValue, ColumnDataType DataType)
{
    public override string ToString()
    {
        return RawValue == null ? "" : RawValue.ToString()!;
    }
}

internal record BooleanScalarValue(bool Value) : ScalarValue(Value, ColumnDataType.Boolean)
{
    public override string ToString()
    {
        return Value.ToString();
    }
}

internal record IntegerScalarValue(long Value) : ScalarValue(Value, ColumnDataType.Integer)
{
    public override string ToString()
    {
        return $"INT64({Value})";
    }
}

internal record FloatScalarValue(float Value) : ScalarValue(Value, ColumnDataType.Decimal)
{
    public override string ToString()
    {
        return $"FLOAT({Value})";
    }
}

internal record StringScalarValue(string Value) : ScalarValue(Value, ColumnDataType.Utf8)
{
    public override string ToString()
    {
        return Value;
    }
}
