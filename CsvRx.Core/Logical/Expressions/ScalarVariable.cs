namespace CsvRx.Core.Logical.Expressions;

internal record ScalarVariable(IEnumerable<string> Names) : ILogicalExpression
{
    public override string ToString()
    {
        return string.Join(".", Names);
    }
}