namespace CsvRx.Logical.Expressions;

public record Alias(ILogicalExpression Expr, string Name) : ILogicalExpression
{
    public override string ToString()
    {
        return $"{Expr} AS {Name}";
    }
}