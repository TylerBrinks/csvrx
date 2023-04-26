using CsvRx.Logical;
using CsvRx.Physical;
using CsvRx.Physical.Rules;

namespace CsvRx;

public class SessionState
{
    public IExecutionPlan CreatePhysicalPlan(ILogicalPlan logicalPlan)
    {
        var optimized = OptimizeLogicalPlan(logicalPlan);

        return new PhysicalPlanner().CreateInitialPlan(optimized);
    }

    public ILogicalPlan OptimizeLogicalPlan(ILogicalPlan logicalPlan)
    {
        logicalPlan = new LogicalPlanOptimizer().Optimize(logicalPlan);

        Console.Write(logicalPlan.ToStringIndented(new Indentation()));
        return logicalPlan;
    }
}

public class LogicalPlanOptimizer
{
    private static readonly List<ILogicalPlanOptimizationRule> Rules = new()
    {
        //new ReplaceDistinctWithAggregate::new()),
        //new SimplifyExpressionsRule(),
        //new(MergeProjection::new()),
        //new(EliminateDuplicatedExpr::new()),
        //new(EliminateFilter::new()),
        //new(EliminateLimit::new()),
        //new(PropagateEmptyRelation::new()),
        //new(FilterNullJoinKeys::default()),
        //new(EliminateOuterJoin::new()),
        //// Filters can't be pushed down past Limits, we should do PushDownFilter after PushDownLimit
        //new(PushDownLimit::new()),
        //new(PushDownFilter::new()),
        //new(SingleDistinctToGroupBy::new()),
        //// The previous optimizations added expressions and projections,
        //// that might benefit from the following rules
        //new(SimplifyExpressions::new()),
        // new(UnwrapCastInComparison::new()),
        // new(CommonSubexprEliminate::new()),
        new PushDownProjectionRule(),
        new EliminateProjectionRule(),
        //// PushDownProjection can push down Projections through Limits, do PushDownLimit again.
        //new(PushDownLimit::new()),
    };

    public ILogicalPlan Optimize(ILogicalPlan logicalPlan)
    {
        var newPlan = logicalPlan;

        foreach (var rule in Rules)
        {
            var result = OptimizeRecursively(rule, newPlan);
            newPlan = result;
        }

        return newPlan;
    }

    private ILogicalPlan OptimizeRecursively(ILogicalPlanOptimizationRule rule, ILogicalPlan plan)
    {
        switch (rule.ApplyOrder)
        {
            case ApplyOrder.TopDown:
                {
                    var optimizeSelfOpt = rule.TryOptimize(plan);
                    ILogicalPlan optimizeInputsOpt = null!;

                    if (optimizeSelfOpt != null)
                    {
                        optimizeInputsOpt = OptimizeInputs(rule, optimizeSelfOpt);
                    }
                    else
                    {
                        optimizeInputsOpt = OptimizeInputs(rule, plan);
                    }

                    return optimizeInputsOpt ?? optimizeSelfOpt;
                }
            case ApplyOrder.BottomUp:
                {
                    ILogicalPlan optimizeInputsOpt = OptimizeInputs(rule, plan);
                    ILogicalPlan optimizeSelfOpt = null!;

                    if (optimizeInputsOpt != null)
                    {
                        optimizeSelfOpt = rule.TryOptimize(optimizeInputsOpt);
                    }
                    else
                    {
                        optimizeSelfOpt = rule.TryOptimize(plan);
                    }

                    return optimizeSelfOpt ?? optimizeInputsOpt;
                }
            default:
                return rule.TryOptimize(plan);
        }
    }

    private ILogicalPlan OptimizeInputs(ILogicalPlanOptimizationRule rule, ILogicalPlan plan)
    {
        var inputs = plan.GetInputs();
        var result = inputs.Select(p => OptimizeRecursively(rule, p)).ToList();

        if (!result.Any() || result.All(r => r == null))//TODO: or all are null
        {
            return null;
        }

        var newInputs = result.Select((p, i) =>
        {
            if (p != null)
            {
                return p;
            }

            return inputs[i];
        }).ToList();

        return plan.WithNewInputs(newInputs);
    }
}

public enum ApplyOrder
{
    None,
    TopDown,
    BottomUp
}


public interface ILogicalPlanOptimizationRule
{
    ApplyOrder ApplyOrder { get; }
    ILogicalPlan TryOptimize(ILogicalPlan plan);
}
