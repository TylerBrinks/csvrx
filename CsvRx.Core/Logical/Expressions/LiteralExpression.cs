using CsvRx.Core.Logical.Values;

namespace CsvRx.Core.Logical.Expressions;

internal record LiteralExpression(ScalarValue Value) : ILogicalExpression
{
    public override string ToString()
    {
        return Value.ToString();
    }
}