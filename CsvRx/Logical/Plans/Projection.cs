using CsvRx.Data;

namespace CsvRx.Logical.Plans;

public record Projection(ILogicalPlan Plan, List<ILogicalExpression> Expr, Schema Schema) : ILogicalPlan
{
    public string ToStringIndented(Indentation? indentation)
    {
        var indent = indentation ?? new Indentation();
        var expressions = Expr.Select(_ => _.ToString()).ToList();

        var projections = string.Join(", ", expressions);
        return $"Projection: {projections} {indent.Next(Plan)}";
    }
}