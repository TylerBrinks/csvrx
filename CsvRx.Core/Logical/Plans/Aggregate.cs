using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;

namespace CsvRx.Core.Logical.Plans;

internal record Aggregate(
    ILogicalPlan Plan,
    List<ILogicalExpression> GroupExpressions,
    List<ILogicalExpression> AggregateExpressions,
    Schema Schema)
    : ILogicalPlanParent
{
    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        var groups = string.Join(",", GroupExpressions);
        var aggregates = string.Join(",", AggregateExpressions);

        return $"Aggregate: groupBy=[{groups}], aggregate=[{aggregates}]{indent.Next(Plan)}";
    }

    public static Aggregate TryNew(ILogicalPlan plan, List<ILogicalExpression> groupExpressions, List<ILogicalExpression> aggregateExpressions)
    {
        var allExpressions = groupExpressions.Concat(aggregateExpressions).ToList();
        var schema = new Schema(allExpressions.ExpressionListToFields(plan));

        return new Aggregate(plan, groupExpressions, aggregateExpressions, schema);
    }
}