using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;

namespace CsvRx.Core.Logical.Rules;

internal class PushDownProjectionRule : ILogicalPlanOptimizationRule
{
    public ApplyOrder ApplyOrder => ApplyOrder.TopDown;

    public ILogicalPlan? TryOptimize(ILogicalPlan plan)
    {
        if (plan is Aggregate aggregate)
        {
            var requiredColumns = new HashSet<Column>();
            foreach (var e in aggregate.AggregateExpressions.Concat(aggregate.GroupExpressions))
            {
                LogicalExtensions.ExprToColumns(e, requiredColumns);
            }

            var newExpr = GetExpr(requiredColumns, aggregate.Plan.Schema);
            var childProjection = Projection.TryNew(aggregate.Plan, newExpr);

            var optimizedChild = TryOptimize(childProjection);

            return (ILogicalPlanParent)plan.WithNewInputs(new List<ILogicalPlan> { optimizedChild });
        }

        if (plan is TableScan { Projection: null } scan)
        {
            return PushDownScan(new HashSet<Column>(), scan);
        }

        if (plan is not Projection projection)
        {
            return null;
        }

        var childPlan = projection.Plan;

        var newPlan = FromChildPlan(childPlan, plan);

        return newPlan;
    }

    private ILogicalPlan FromChildPlan(ILogicalPlan childPlan, ILogicalPlan projection)
    {
        var empty = !projection.GetExpressions().Any();

        switch (childPlan)
        {
            case Projection p:
                {
                    var newPlan = new Projection(p.Plan, projection.GetExpressions().ToList(), projection.Schema);
                    return TryOptimize(newPlan);
                }
            case Aggregate a:
                {
                    var requiredColumns = new HashSet<Column>();
                    LogicalExtensions.ExprListToColumns(projection.GetExpressions(), requiredColumns);
                    var newAggrExpr = new List<ILogicalExpression>();

                    foreach (var aggExpr in a.AggregateExpressions)
                    {
                        var col = new Column(aggExpr.CreateName());
                        if (requiredColumns.Contains(col))
                        {
                            newAggrExpr.Add(aggExpr);
                        }
                    }

                    if (!newAggrExpr.Any() && a.AggregateExpressions.Count == 1)
                    {
                        throw new NotImplementedException();
                    }

                    var newAgg = Aggregate.TryNew(a.Plan, a.GroupExpressions, newAggrExpr);

                    return GeneratePlan(empty, projection, newAgg);
                }
            case Filter f:
                {
                    if (CanEliminate(projection, childPlan.Schema))
                    {
                        // should projection be 'plan'?
                        var newProj = projection.WithNewInputs(new List<ILogicalPlan> { f.Plan });
                        return childPlan.WithNewInputs(new List<ILogicalPlan> { newProj });
                    }
                    var requiredColumns = new HashSet<Column>();
                    LogicalExtensions.ExprListToColumns(projection.GetExpressions(), requiredColumns);
                    LogicalExtensions.ExprListToColumns(new List<ILogicalExpression> { f.Predicate }, requiredColumns);

                    var newExpr = GetExpr(requiredColumns, f.Plan.Schema);
                    var newProjection = Projection.TryNew(f.Plan, newExpr);
                    var newFilter = childPlan.WithNewInputs(new List<ILogicalPlan> { newProjection });

                    return GeneratePlan(empty, projection, newFilter);
                }
            case TableScan t:
                {
                    var usedColumns = new HashSet<Column>();

                    foreach (var expr in projection.GetExpressions())
                    {
                        LogicalExtensions.ExprToColumns(expr, usedColumns);
                    }

                    var scan = PushDownScan(usedColumns, t);

                    return projection.WithNewInputs(new List<ILogicalPlan> { scan });
                }
            default:
                throw new NotImplementedException();
        }
    }

    private ILogicalPlan PushDownScan(HashSet<Column> usedColumns, TableScan tableScan)
    {
        var projection = usedColumns.Select(c => tableScan.Source.Schema.IndexOfColumn(c)!.Value).ToList();
        var fields = projection.Select(i => tableScan.Source.Schema.Fields[i]).ToList();
        var schema = new Schema(fields);

        return tableScan with { Schema = schema, Projection = projection };
    }

    ILogicalPlan GeneratePlan(bool empty, ILogicalPlan plan, ILogicalPlan newPlan)
    {
        if (empty)
        {
            return newPlan;
        }

        return plan.WithNewInputs(new List<ILogicalPlan> { newPlan });
    }

    private bool CanEliminate(ILogicalPlan projection, Schema schema)
    {
        var expressions = projection.GetExpressions();
        if (expressions.Count != schema.Fields.Count)
        {
            return false;
        }

        for (var i = 0; i < expressions.Count; i++)
        {
            var expr = expressions[i];

            if (expr is Column c)
            {
                var field = schema.Fields[i];
                if (c.Name != field.Name)
                {
                    return false;
                }

            }

            return false;
        }

        return true;
    }

    private List<ILogicalExpression> GetExpr(HashSet<Column> columns, Schema schema)
    {
        var expr = schema.Fields.Select(f => (ILogicalExpression)new Column(f.Name))
            .Where(columns.Contains)
            .ToList();

        return expr;
    }
}