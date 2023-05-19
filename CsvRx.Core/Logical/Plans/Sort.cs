using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;

namespace CsvRx.Core.Logical.Plans;

internal record Sort(ILogicalPlan Plan, List<ILogicalExpression> OrderByExpressions) : ILogicalPlanParent
{
    internal static ILogicalPlan TryNew(ILogicalPlan plan, List<ILogicalExpression> orderByExpressions)
    {
        var expressions = RewriteByAggregates(orderByExpressions, plan);

        var missingColumns = new HashSet<Column>();

        var missingExpressions = expressions.Select(expr => expr.ToColumns())
            .SelectMany(columns => columns.Where(column => plan.Schema.FieldFromColumn(column) == null));

        foreach (var column in missingExpressions)
        {
            missingColumns.Add(column);
        }

        if (!missingColumns.Any())
        {
            return new Sort(plan, expressions);
        }

        var newExpressions = plan.Schema.Fields.Select(f => (ILogicalExpression)f.QualifiedColumn()).ToList();

        plan = plan.AddMissingColumns(missingColumns, false);

        var sort = new Sort(plan, expressions.NormalizeColumn(plan));

        return new Projection(sort, newExpressions, plan.Schema);
    }

    private static List<ILogicalExpression> RewriteByAggregates(IEnumerable<ILogicalExpression> orderByExpressions, ILogicalPlan plan)
    {
        return orderByExpressions.Select(e =>
        {
            if (e is OrderBy order)
            {
                return order with {Expression = RewriteByAggregates(order.Expression, plan)};
            }

            return e;
        }).ToList();
    }

    private static ILogicalExpression RewriteByAggregates(ILogicalExpression expression, ILogicalPlan plan)
    {
        var inputs = plan.GetInputs();

        if (inputs.Count == 1)
        {
            var projectedExpressions = plan.GetExpressions();
            return RewriteForProjection(expression, projectedExpressions, inputs[0]);
        }

        return expression;
    }

    private static ILogicalExpression RewriteForProjection(
        ILogicalExpression expression, 
        List<ILogicalExpression> projectionExpressions, 
        ILogicalPlan plan)
    {
        return expression.Transform(expression, e =>
        {
            var found = projectionExpressions.Find(ex => ex == expression);
            if (found != null)
            {
                var column = found.ToField(plan.Schema).QualifiedColumn();
                return column;
            }

            var name = expression.CreateName();
            var searchColumn = new Column(name);

            var foundMatch = projectionExpressions.Find(_ => searchColumn == _);
            if (foundMatch != null)
            {
                //TODO cast & try cast
                return foundMatch;
            }

            return e;
        });
    }

    public Schema Schema => Plan.Schema;

    public string ToStringIndented(Indentation? indentation)
    {
        var indent = indentation ?? new Indentation();
        return $"Sort: {indent.Next(Plan)}";
    }
}