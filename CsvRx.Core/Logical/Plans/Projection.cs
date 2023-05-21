using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;

namespace CsvRx.Core.Logical.Plans;

internal record Projection(ILogicalPlan Plan, List<ILogicalExpression> Expression, Schema Schema) : ILogicalPlanParent
{
    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        var expressions = Expression.Select(_ => _.ToString()).ToList();

        var projections = string.Join(", ", expressions);

        return $"Projection: {projections} {indent.Next(Plan)}";
    }

    public static Projection TryNew(ILogicalPlan plan, List<ILogicalExpression> expressions)
    {
        var schema = new Schema(expressions.ExpressionListToFields(plan));

        return new Projection(plan, expressions, schema);
    }
}