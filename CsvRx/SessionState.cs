using CsvRx.Data;
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
        // logicalPlan = new LogicalPlanAnalyzer().Check(logicalPlan);

        logicalPlan = new LogicalPlanOptimizer().Optimize(logicalPlan);

        Console.Write(logicalPlan.ToStringIndented(new Indentation()));
        return logicalPlan;
    }
}

public class LogicalPlanOptimizer
{
    private static readonly List<ILogicalPlanOptimizationRule> Rules = new()
    {
        //Arc::new(SimplifyExpressions::new()),
        // Arc::new(UnwrapCastInComparison::new()),
        //new ReplaceDistinctWithAggregate::new()),
        // Arc::new(DecorrelateWhereExists::new()),
        // Arc::new(DecorrelateWhereIn::new()),
        // Arc::new(ScalarSubqueryToJoin::new()),
        // Arc::new(ExtractEquijoinPredicate::new()),
        //// simplify expressions does not simplify expressions in subqueries, so we
        //// run it again after running the optimizations that potentially converted
        //// subqueries to joins
        //new SimplifyExpressionsRule(),
        //new(MergeProjection::new()),
        //Arc::new(RewriteDisjunctivePredicate::new()),
        //new(EliminateDuplicatedExpr::new()),
        //new(EliminateFilter::new()),
        // Arc::new(EliminateCrossJoin::new()),
        // Arc::new(CommonSubexprEliminate::new()),
        //new(EliminateLimit::new()),
        // Arc::new(PropagateEmptyRelation::new()),
        // Arc::new(FilterNullJoinKeys::default()),
        // Arc::new(EliminateOuterJoin::new()),
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
        //// PushDownProjection can pushdown Projections through Limits, do PushDownLimit again.
        //new(PushDownLimit::new()),
    };

    public ILogicalPlan Optimize(ILogicalPlan logicalPlan)
    {
        var newPlan = logicalPlan;

        var previousPlans = new HashSet<ILogicalPlan>();
        previousPlans.Add(newPlan);

        // wrap in passes?

        foreach (var rule in Rules)
        {
            var result = OptimizeRecursively(rule, newPlan);
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


                    Console.WriteLine("");
                    Console.WriteLine("Inputs Opt");
                    if (optimizeInputsOpt == null)
                    {
                        Console.WriteLine("None");
                    }
                    else
                    {
                        Console.WriteLine(optimizeInputsOpt.ToStringIndented(new Indentation()));
                    }

                    Console.WriteLine("Self Opt");
                    if (optimizeSelfOpt == null)
                    {
                        Console.WriteLine("None");
                    }
                    else
                    {
                        Console.WriteLine(optimizeSelfOpt.ToStringIndented(new Indentation()));
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


//public class SimplifyExpressionsRule : ILogicalPlanOptimizationRule
//{
//    public ApplyOrder ApplyOrder => ApplyOrder.None;
//    public ILogicalPlan TryOptimize(ILogicalPlan plan)
//    {
//        //    //var childrenMergeSchema = MergeSchema(plan.GetInputs());
//        //    //var schemas = new List<Schema> { plan.Schema, childrenMergeSchema };

//        //    //var info = schemas.Aggregate(new SimplifyContext(), (context, schema) => context.WithSchema(schema));

//        //    //var simplifier = new ExprSimplifier(info);

//        //    var newInputs = plan.GetInputs().Select(TryOptimize).ToList();
//        //    var expr = plan.GetExpressions()/*.Select(e =>
//        //    {
//        //        var name = e.CreateName();
//        //        var newExpression = simplifier.Simplify(e);
//        //        var newName = newExpression.CreateName();

//        //        if (name != newName)
//        //        {
//        //            return newExpression;//todo alias from name variable
//        //        }

//        //        return newExpression;
//        //    })*/.ToList();

//        //    return plan.FromPlan(expr, newInputs);

//        return plan;
//    }

//    Schema MergeSchema(List<ILogicalPlan> plans)
//    {
//        switch (plans.Count)
//        {
//            case 0:
//                return Schema.Empty();
//            case 1:
//                return plans[0].Schema;
//            default:
//                {
//                    //TODO duplicates?
//                    var merged = plans.Select(p => p.Schema).Aggregate((a, b) => a.Merge(b));
//                    return merged;
//                }
//        }
//    }
//}

//internal class ExprSimplifier
//{
//    private readonly SimplifyContext _context;

//    public ExprSimplifier(SimplifyContext context)
//    {
//        _context = context;
//    }

//    //public ILogicalExpression Simplify(ILogicalExpression expr)
//    //{
//    //    //var simplifier = new Simp
//    //    var constEvaluator = new ConstEvalulator(this);

//    //    expr.Rewrite(constEvaluator)
//    //        .Rewrite(this)
//    //        .Rewrite(constEvaluator)
//    //        .Rewrite(this);
//    //    return expr;
//    //}
//}

//internal class ConstEvalulator : IRewriter<ILogicalExpression>
//{
//    public ConstEvalulator(ExprSimplifier exprSimplifier)
//    {
//        throw new NotImplementedException();
//    }
//}

//internal class SimplifyContext
//{
//    private readonly List<Schema> _schemas = new();

//    internal SimplifyContext WithSchema(Schema schema)
//    {
//        _schemas.Add(schema);
//        return this;
//    }
//}
