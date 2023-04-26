namespace CsvRx.Core.Logical.Expressions;

internal record Wildcard : ILogicalExpression
{
    public override string ToString()
    {
        return "*";
    }
}