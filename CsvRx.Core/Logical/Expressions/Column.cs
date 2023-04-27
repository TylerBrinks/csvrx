namespace CsvRx.Core.Logical.Expressions;

internal record Column(string Name) : LogicalExpression
{
    public override string ToString()
    {
        return Name;
    }
}