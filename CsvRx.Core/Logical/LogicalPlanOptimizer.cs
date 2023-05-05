using CsvRx.Core.Logical.Rules;

namespace CsvRx.Core.Logical;

internal class LogicalPlanOptimizer
{
    private static readonly List<ILogicalPlanOptimizationRule> Rules = new()
    {
        new ReplaceDistinctWithAggregateRule(),
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

    public ILogicalPlan? Optimize(ILogicalPlan logicalPlan)
    {
        var newPlan = logicalPlan;

        foreach (var result in Rules.Select(rule => OptimizeRecursively(rule, newPlan)))
        {
            newPlan = result;
        }

        return newPlan;
    }

    private ILogicalPlan? OptimizeRecursively(ILogicalPlanOptimizationRule rule, ILogicalPlan plan)
    {
        switch (rule.ApplyOrder)
        {
            case ApplyOrder.TopDown:
                {
                    var optimizeSelfOpt = rule.TryOptimize(plan);
                    var optimizeInputsOpt = OptimizeInputs(rule, optimizeSelfOpt ?? plan);

                    return optimizeInputsOpt ?? optimizeSelfOpt;
                }
            case ApplyOrder.BottomUp:
                {
                    var optimizeInputsOpt = OptimizeInputs(rule, plan);
                    var optimizeSelfOpt = rule.TryOptimize(optimizeInputsOpt ?? plan);

                    return optimizeSelfOpt ?? optimizeInputsOpt;
                }
            default:
                return rule.TryOptimize(plan);
        }
    }

    private ILogicalPlan? OptimizeInputs(ILogicalPlanOptimizationRule rule, ILogicalPlan plan)
    {
        var inputs = plan.GetInputs();
        var result = inputs.Select(p => OptimizeRecursively(rule, p)).ToList();

        if (!result.Any() || result.All(r => r == null))//TODO: or all are null
        {
            return null;
        }

        var newInputs = result.Select((p, i) => p ?? inputs[i]).ToList();

        return plan.WithNewInputs(newInputs);
    }
}