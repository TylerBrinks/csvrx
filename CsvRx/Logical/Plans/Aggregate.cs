using CsvRx.Data;

namespace CsvRx.Logical.Plans;

internal record Aggregate(
    ILogicalPlan Plan,
    List<ILogicalExpression> GroupExpressions,
    List<ILogicalExpression> AggregateExpressions,
    Schema Schema)
    : ILogicalPlanWrapper
{
    public string ToStringIndented(Indentation? indentation)
    {
        var indent = indentation ?? new Indentation();
        var groups = string.Join(",", GroupExpressions);
        var aggregates = string.Join(",", AggregateExpressions);
        return $"Aggregate: groupBy=[{groups}], aggr=[{aggregates}]{indent.Next(Plan)}";
    }

    public static Aggregate TryNew(ILogicalPlan plan, List<ILogicalExpression> groupExpressions, List<ILogicalExpression> aggregateExpressions)
    {
        var allExpressions = groupExpressions.Concat(aggregateExpressions).ToList();
        var schema = new Schema(Extensions.ExprListToFields(allExpressions, plan));
        return new Aggregate(plan, groupExpressions, aggregateExpressions, schema);
    }
}
