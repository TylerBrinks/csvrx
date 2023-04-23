using CsvRx.Logical;

namespace CsvRx.Physical.Rules;

public class EliminateProjectionRule : ILogicalPlanOptimizationRule
{
    public ApplyOrder ApplyOrder => ApplyOrder.TopDown;

    public ILogicalPlan TryOptimize(ILogicalPlan plan)
    {
        return plan;
    }
}