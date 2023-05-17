using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;

namespace CsvRx.Core.Logical.Rules;

internal class PushDownProjectionRule : ILogicalPlanOptimizationRule
{
    public ApplyOrder ApplyOrder => ApplyOrder.TopDown;

    public ILogicalPlan? TryOptimize(ILogicalPlan plan)
    {
        switch (plan)
        {
            case Aggregate aggregate:
                {
                    var requiredColumns = new HashSet<Column>();
                    foreach (var e in aggregate.AggregateExpressions.Concat(aggregate.GroupExpressions))
                    {
                        e.ExpressionToColumns(requiredColumns);
                    }

                    var newExpression = GetExpression(requiredColumns, aggregate.Plan.Schema);
                    var childProjection = Projection.TryNew(aggregate.Plan, newExpression);

                    var optimizedChild = TryOptimize(childProjection);

                    var newInputs = new List<ILogicalPlan>();

                    if (optimizedChild != null)
                    {
                        newInputs.Add(optimizedChild);
                    }

                    return (ILogicalPlanParent)plan.WithNewInputs(newInputs);
                }
            case TableScan { Projection: null } scan:
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

    private ILogicalPlan? FromChildPlan(ILogicalPlan childPlan, ILogicalPlan projection)
    {
        var empty = !projection.GetExpressions().Any();

        switch (childPlan)
        {
            case Projection p:
                {
                    var replaceMap = CollectProjectionExpressions(p);

                    var newExpressions = projection.GetExpressions()
                        .Select(e => ReplaceColumnsByName(e, replaceMap))
                        .Select((e, i) =>
                        {
                            var parentName = projection.Schema.Fields[i].Name; // removed f!.name

                            return e.CreateName() == parentName ? e : new Alias(e, parentName);
                        })
                        .ToList();

                    var newPlan = new Projection(p.Plan, newExpressions, projection.Schema);
                    return TryOptimize(newPlan);
                }
            case Aggregate a:
                {
                    var requiredColumns = new HashSet<Column>();
                    projection.GetExpressions().ExpressionListToColumns(requiredColumns);
                    var newAggregate = (
                        from agg in a.AggregateExpressions
                        let col = new Column(agg.CreateName())
                        where requiredColumns.Contains(col)
                        select agg).ToList();

                    if (!newAggregate.Any() && a.AggregateExpressions.Count == 1)
                    {
                        throw new InvalidOperationException();
                    }

                    var newAgg = Aggregate.TryNew(a.Plan, a.GroupExpressions, newAggregate);

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
                    projection.GetExpressions().ExpressionListToColumns(requiredColumns);
                    new List<ILogicalExpression> { f.Predicate }.ExpressionListToColumns(requiredColumns);

                    var newExpression = GetExpression(requiredColumns, f.Plan.Schema);
                    var newProjection = Projection.TryNew(f.Plan, newExpression);
                    var newFilter = childPlan.WithNewInputs(new List<ILogicalPlan> { newProjection });

                    return GeneratePlan(empty, projection, newFilter);
                }
            case TableScan t:
                {
                    var usedColumns = new HashSet<Column>();

                    foreach (var expr in projection.GetExpressions())
                    {
                        expr.ExpressionToColumns(usedColumns);
                    }

                    var scan = PushDownScan(usedColumns, t);

                    return projection.WithNewInputs(new List<ILogicalPlan> { scan });
                }
            case SubqueryAlias a:
                {
                    var replaceMap = GenerateColumnReplaceMap(a);
                    var requiredColumns = new HashSet<Column>();
                    var expr = ((Projection)projection).Expression;

                    expr.ExpressionListToColumns(requiredColumns);

                    var newRequiredColumns = requiredColumns.Select(c => replaceMap[c]).ToList();
                    var newExpression = GetExpression(newRequiredColumns, a.Plan.Schema);
                    var newProjection = Projection.TryNew(a.Plan, newExpression);
                    var newAlias = childPlan.WithNewInputs(new List<ILogicalPlan> { newProjection });

                    return GeneratePlan(empty, projection, newAlias);
                }
            case Join j:
                {
                    var pushColumns = new HashSet<Column>();
                    foreach (var expr in projection.GetExpressions())
                    {
                        expr.ExpressionToColumns(pushColumns);
                    }

                    foreach (var (left, right) in j.On)
                    {
                        left.ExpressionToColumns(pushColumns);
                        right.ExpressionToColumns(pushColumns);
                    }

                    j.Filter?.ExpressionToColumns(pushColumns);

                    var newLeft = GenerateProjection(pushColumns, j.Plan.Schema, j.Plan);
                    var newRight = GenerateProjection(pushColumns, j.Right.Schema, j.Right);
                    var newJoin = childPlan.WithNewInputs(new List<ILogicalPlan> { newLeft, newRight });
                    return GeneratePlan(empty, projection, newJoin);
                }
            default:
                throw new NotImplementedException("FromChildPlan plan type not implemented yet");
        }
    }

    private static ILogicalPlan GenerateProjection(IReadOnlySet<Column> usedColumns, Schema schema, ILogicalPlan plan)
    {
        var columns = schema.Fields.Select(f =>
        {
            var column = f.QualifiedColumn();
            if (usedColumns.Contains(column))
            {
                return (ILogicalExpression)column;
            }

            return null;
        })
        .Where(c => c != null)
        .ToList();

        return Projection.TryNew(plan, columns!);
    }

    private static Dictionary<Column, Column> GenerateColumnReplaceMap(SubqueryAlias alias)
    {
        return alias.Plan.Schema.Fields.Select((f, i) =>
            (
                alias.Schema.Fields[i].QualifiedColumn(),
                f.QualifiedColumn())
            )
            .ToDictionary(d => d.Item1, d => d.Item2);
    }

    private static Dictionary<string, ILogicalExpression> CollectProjectionExpressions(Projection projection)
    {
        return projection.Schema.Fields.Select((f, i) =>
        {
            var expr = projection.Expression[i] switch
            {
                Alias a => a.Expression,
                _ => projection.Expression[i]
            };

            return (f.Name, Expr: expr);// removed f!.Name
        })
        .ToDictionary(f => f.Name, f => f.Expr);
    }

    private static ILogicalExpression ReplaceColumnsByName(ILogicalExpression expression, Dictionary<string, ILogicalExpression> replaceMap)
    {
        return expression.Transform(expression, e =>
        {
            if (e is Column c)
            {
                return replaceMap[c.Name];
            }

            return expression;
        });
    }

    private static ILogicalPlan PushDownScan(IEnumerable<Column> usedColumns, TableScan tableScan)
    {
        var projection = usedColumns
            .Where(c => c.Relation == null || c.Relation.Name == tableScan.Name)
            .Select(c =>
                {
                    var index = tableScan.Source.Schema!.IndexOfColumn(c);
                    if (index == null)
                    {
                        return -1;
                    }
                    return index.Value;
                })
            .Where(i => i > -1)
            .ToList();

        var fields = projection
            .Select(i => tableScan.Source.Schema!.Fields[i].FromQualified(new TableReference(tableScan.Name)))
            .ToList();
        //var fields = projection.Select(i => tableScan.Schema.Fields[i]).ToList();

        var schema = new Schema(fields);

        return tableScan with { Schema = schema, Projection = projection };
    }

    private static ILogicalPlan GeneratePlan(bool empty, ILogicalPlan plan, ILogicalPlan newPlan)
    {
        return empty ? newPlan : plan.WithNewInputs(new List<ILogicalPlan> { newPlan });
    }

    private static bool CanEliminate(ILogicalPlan projection, Schema schema)
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
            else
            {
                return false;
            }
        }

        return true;
    }

    private static List<ILogicalExpression> GetExpression(IEnumerable<Column> columns, Schema schema)
    {
        var expr = schema.Fields.Select(f => (ILogicalExpression)f.QualifiedColumn())
            .Where(columns.Contains)
            .ToList();

        return expr;
    }
}