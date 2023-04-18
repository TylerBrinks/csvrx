using CsvRx.Data;

namespace CsvRx.Logical;

public record Projection(ILogicalPlan Plan, List<ILogicalExpression> Expr, Schema Schema) : ILogicalPlan
{
    public string ToStringIndented(Indentation? indentation)
    {
        var indent = indentation ?? new Indentation();
        var exprs = Expr.Select(_ => _.ToString()).ToList();

        var projections = string.Join(", ", exprs);
        return $"Projection: {projections} {indent.Next(Plan)}";
    }
}