using CsvRx.Data;

namespace CsvRx.Logical;

public interface ILogicalPlan
{
    Schema Schema { get; }
}

public interface ILogicalExpression
{

}

public record Projection(ILogicalPlan Input, List<ILogicalExpression> Expr, Schema Schema) : ILogicalPlan
{
}