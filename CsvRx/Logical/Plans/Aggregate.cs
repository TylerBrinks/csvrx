using CsvRx.Data;

namespace CsvRx.Logical.Plans;

internal record Aggregate(
    ILogicalPlan Plan,
    List<ILogicalExpression> GroupExpressions,
    List<ILogicalExpression> AggregateExpressions,
    Schema Schema)
    : ILogicalPlan
{
    public string ToStringIndented(Indentation? indentation)
    {
        var indent = indentation ?? new Indentation();
        var groups = string.Join(",", GroupExpressions);
        var aggregates = string.Join(",", AggregateExpressions);
        return $"Aggregate: groupBy=[{groups}], aggr=[{aggregates}]{indent.Next(Plan)}";
    }
}
