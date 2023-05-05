using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;

namespace CsvRx.Core.Logical.Rules;

internal class EliminateProjectionRule : ILogicalPlanOptimizationRule
{
    public ApplyOrder ApplyOrder => ApplyOrder.TopDown;

    public ILogicalPlan? TryOptimize(ILogicalPlan plan)
    {
        switch (plan)
        {
            case Projection p:
                var childPlan = p.Plan;
                switch (childPlan)
                {
                    case Filter:
                    case TableScan:
                    case Sort:
                        // sort
                        return CanEliminate(p, childPlan.Schema) ? childPlan : plan;

                    default:
                        return plan.Schema.Equals(childPlan.Schema) ? childPlan : null;
                }

            default:
                return null;
        }
    }

    private bool CanEliminate(Projection projection, Schema schema)
    {
        if (projection.Expression.Count != schema.Fields.Count)
        {
            return false;
        }

        for (var i = 0; i < projection.Expression.Count; i++)
        {
            var expr = projection.Expression[i];
            if (expr is Column c)
            {
                var d = schema.Fields[i];
                if (c.Name != d.Name)
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        return true;
    }
}