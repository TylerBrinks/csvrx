﻿using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;

namespace CsvRx.Core.Logical.Plans;

internal record Projection(ILogicalPlan Plan, List<ILogicalExpression> Expr, Schema Schema) : ILogicalPlanParent
{
    public string ToStringIndented(Indentation? indentation)
    {
        var indent = indentation ?? new Indentation();
        var expressions = Expr.Select(_ => _.ToString()).ToList();

        var projections = string.Join(", ", expressions);
        return $"Projection: {projections} {indent.Next(Plan)}";
    }

    public static Projection TryNew(ILogicalPlan plan, List<ILogicalExpression> expressions)
    {
        var schema = new Schema(LogicalExtensions.ExprListToFields(expressions, plan));
        return new Projection(plan, expressions, schema);
    }
}