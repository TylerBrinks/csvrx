namespace CsvRx.Logical.Expressions;

public record ScalarVariable(IEnumerable<string> Names) : ILogicalExpression
{
    public override string ToString()
    {
        return string.Join(".", Names);
    }
}