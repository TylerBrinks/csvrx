using CsvRx.Core.Data;

namespace CsvRx.Core.Logical.Plans;

internal record Limit(ILogicalPlan Plan, int? Skip = 0, int? Fetch = 0) : ILogicalPlanParent
{
    public Schema Schema => Plan.Schema;

    public override string ToString()
    {
        return $"Limit: Skip {Skip}, Limit {Fetch}";
    }

    public string ToStringIndented(Indentation? indentation)
    {
        var indent = indentation ?? new Indentation();
        return $"Limit: Skip {Skip}, Limit {Fetch}";
    }
}