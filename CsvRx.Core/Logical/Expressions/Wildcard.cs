namespace CsvRx.Core.Logical.Expressions;

internal record Wildcard : LogicalExpression
{
    public override string ToString()
    {
        return "*";
    }
}