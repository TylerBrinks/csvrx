using CsvRx.Logical.Functions;

namespace CsvRx.Logical.Expressions;

internal record LiteralExpression(string Value) : ILogicalExpression
{
    public override string ToString()
    {
        return Value;
    }
}

internal record AggregateFunctionExpression(AggregateFunction AggregateFunction, List<ILogicalExpression> ExpressionArgs) : ILogicalExpression
{
    public override string ToString()
    {
        var exp = string.Join(", ", ExpressionArgs.Select(_ => _.ToString()));
        return $"{AggregateFunction}({exp})";
    }
}

internal record Wildcard : ILogicalExpression
{
    public override string ToString()
    {
        return "*";
    }
}