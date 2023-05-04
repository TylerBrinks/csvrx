namespace CsvRx.Core.Logical.Expressions;

internal record Alias(ILogicalExpression Expression, string Name) : ILogicalExpression
{
    public override string ToString()
    {
        return $"{Expression} AS {Name}";
    }
}