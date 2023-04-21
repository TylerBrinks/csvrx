namespace CsvRx.Logical.Expressions;

internal record Wildcard : ILogicalExpression
{
    public override string ToString()
    {
        return "*";
    }
}