namespace CsvRx.Core.Logical.Expressions;

internal record OrderBy(ILogicalExpression Expression, bool Ascending /*nulls first?*/ ) : ILogicalExpression
{
    public override string ToString()
    {
        var direction = Ascending ? "Asc" : "Desc";
        return $"Order By {Expression.CreateName()} {direction}";
    }
}