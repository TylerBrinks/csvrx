﻿using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;

namespace CsvRx.Core.Logical.Plans;

internal record Sort(ILogicalPlan Plan, List<ILogicalExpression> OrderByExpressions) : ILogicalPlanParent
{
    internal static ILogicalPlan TryNew(ILogicalPlan plan, List<ILogicalExpression> orderByExpressions)
    {
        var expressions = RewriteByAggregates(orderByExpressions, plan);

        var missingColumns = new HashSet<Column>();
        LogicalExtensions.ExprListToColumns(expressions, missingColumns);

        if (!missingColumns.Any())
        {
            return new Sort(plan, expressions);
        }

        var newExpressions = plan.Schema.Fields.Select(f => (ILogicalExpression)new Column(f.Name)).ToList();

        var sort = new Sort(plan, newExpressions);

        return new Projection(sort, newExpressions, plan.Schema);
    }

    private static List<ILogicalExpression> RewriteByAggregates(List<ILogicalExpression> orderByExpressions, ILogicalPlan plan)
    {
        return orderByExpressions.Select(e =>
        {
            if (e is OrderBy o)
            {
                return new OrderBy(RewriteByAggregates(o.Expression, plan), o.Ascending);
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
            var found = projectionExpressions.Find(e => e == expression);
            if (found != null)
            {
                var column = new Column(LogicalExtensions.ToField(found, plan.Schema).Name);
                return column;
            }

            var name = expression.CreateName();
            var searchColumn = new Column(name);

            var foundMatch = projectionExpressions.Find(e => searchColumn == e);
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