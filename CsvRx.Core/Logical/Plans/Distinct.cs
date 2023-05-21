using CsvRx.Core.Data;

namespace CsvRx.Core.Logical.Plans;

internal record Distinct(ILogicalPlan Plan) : ILogicalPlanParent
{
    public Schema Schema => Plan.Schema;

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        return $"Distinct: {indent.Next(Plan)}";
    }
}