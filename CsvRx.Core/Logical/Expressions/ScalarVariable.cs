namespace CsvRx.Core.Logical.Expressions;

internal record ScalarVariable(IEnumerable<string> Names) : LogicalExpression
{
    public override string ToString()
    {
        return string.Join(".", Names);
    }
}