namespace CsvRx.Logical.Expressions;

internal record LiteralExpression(string Value) : ILogicalExpression
{
    public override string ToString()
    {
        return Value;
    }
}