namespace CsvRx.Core.Logical.Expressions;

internal record Column(string Name) : ILogicalExpression
{
    public override string ToString()
    {
        return Name;
    }
}