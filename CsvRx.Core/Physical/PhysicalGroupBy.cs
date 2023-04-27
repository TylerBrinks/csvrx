using CsvRx.Core.Physical.Expressions;

namespace CsvRx.Core.Physical;

internal record PhysicalGroupBy(
    List<(IPhysicalExpression Expression, string Name)> Expr,
    List<(IPhysicalExpression Expression, string Name)> NullExpressions,
    List<List<bool>> Groups)
{
    public static PhysicalGroupBy NewSingle(List<(IPhysicalExpression Expression, string Name)> expressions)
    {
        return new PhysicalGroupBy(
            expressions,
            new List<(IPhysicalExpression Expression, string Name)>(),
            new List<List<bool>>(expressions.Count));
    }
}