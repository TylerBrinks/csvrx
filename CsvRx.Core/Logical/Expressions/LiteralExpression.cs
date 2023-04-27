namespace CsvRx.Core.Logical.Expressions;

internal record LiteralExpression(ScalarValue Value) : LogicalExpression
{
    public override string ToString()
    {
        return Value.ToString();
    }
}