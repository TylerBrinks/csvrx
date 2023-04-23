using CsvRx.Data;

namespace CsvRx.Logical.Plans;

internal record Filter(ILogicalPlan Plan, ILogicalExpression Predicate) : ILogicalPlanWrapper
{
    public Schema Schema => Plan.Schema;

    public override string ToString()
    {
        return $"Filter: {Predicate}::{Plan}";
    }

    public string ToStringIndented(Indentation? indentation)
    {
        var indent = indentation ?? new Indentation();
        return $"Filter: {Predicate}{indent.Next(Plan)}";
    }
}