namespace CsvRx.Core.Logical.Expressions;

internal record Alias(LogicalExpression Expr, string Name) : LogicalExpression
{
    public override string ToString()
    {
        return $"{Expr} AS {Name}";
    }
}