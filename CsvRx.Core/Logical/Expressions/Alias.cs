namespace CsvRx.Core.Logical.Expressions;

internal record Alias(ILogicalExpression Expr, string Name) : ILogicalExpression
{
    public override string ToString()
    {
        return $"{Expr} AS {Name}";
    }
}