using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;

namespace CsvRx.Core.Logical.Plans;

internal record Filter(ILogicalPlan Plan, ILogicalExpression Predicate) : ILogicalPlanParent
{
    public Schema Schema => Plan.Schema;

    public override string ToString()
    {
        return $"Filter: {Predicate}::{Plan}";
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        return $"Filter: {Predicate}{indent.Next(Plan)}";
    }
}