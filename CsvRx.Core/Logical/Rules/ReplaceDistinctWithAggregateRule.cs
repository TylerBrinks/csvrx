using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;

namespace CsvRx.Core.Logical.Rules;

internal class ReplaceDistinctWithAggregateRule : ILogicalPlanOptimizationRule
{
    public ApplyOrder ApplyOrder => ApplyOrder.BottomUp;

    public ILogicalPlan TryOptimize(ILogicalPlan plan)
    {
        if (plan is not Distinct d)
        {
            return plan;
        }

        var groupExpression = plan.Schema.ExpandWildcard();
        return Aggregate.TryNew(d.Plan, groupExpression, new List<ILogicalExpression>());
    }
}
