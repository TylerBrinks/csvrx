﻿using CsvRx.Core.Data;

namespace CsvRx.Core.Logical.Plans;

internal record Projection(ILogicalPlan Plan, List<LogicalExpression> Expr, Schema Schema) : ILogicalPlanWrapper
{
    public string ToStringIndented(Indentation? indentation)
    {
        var indent = indentation ?? new Indentation();
        var expressions = Expr.Select(_ => _.ToString()).ToList();

        var projections = string.Join(", ", expressions);
        return $"Projection: {projections} {indent.Next(Plan)}";
    }

    public static Projection TryNew(ILogicalPlan plan, List<LogicalExpression> expressions)
    {
        var schema = new Schema(Extensions.ExprListToFields(expressions, plan));
        return new Projection(plan, expressions, schema);
    }
}