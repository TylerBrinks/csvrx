using CsvRx.Core.Physical.Expressions;

namespace CsvRx.Core.Physical;

internal record GroupBy(
    List<(IPhysicalExpression Expression, string Name)> Expr,
    List<(IPhysicalExpression Expression, string Name)> NullExpressions,
    List<List<bool>> Groups)
{
    public static GroupBy NewSingle(List<(IPhysicalExpression Expression, string Name)> expressions)
    {
        return new GroupBy(
            expressions,
            new List<(IPhysicalExpression Expression, string Name)>(),
            new List<List<bool>>(expressions.Count));
    }
}