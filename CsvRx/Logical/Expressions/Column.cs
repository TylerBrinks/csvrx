namespace CsvRx.Logical.Expressions;

public record Column(string Name) : ILogicalExpression
{
    public override string ToString()
    {
        return Name;
    }
}

public record ScalarVariable(IEnumerable<string> Names) : ILogicalExpression
{
    public override string ToString()
    {
        return string.Join(".", Names);
    }
}

public record Alias(ILogicalExpression Expr, string Name) : ILogicalExpression
{
    public override string ToString()
    {
        return $"{Expr} AS {Name}";
    }
}