using CsvRx.Data;

namespace CsvRx.Logical;

public class Aggregate : ILogicalPlan
{
    private readonly ILogicalPlan _plan;
    private readonly List<ILogicalExpression> _groupExpr;
    private readonly List<LogicalAggregateExpression> _aggregateExpr;

    public Aggregate(ILogicalPlan plan, List<ILogicalExpression> groupExpr, List<LogicalAggregateExpression> aggregateExpr)
    {
        _plan = plan;
        _groupExpr = groupExpr;
        _aggregateExpr = aggregateExpr;
    }

    public Schema Schema => new(_groupExpr.Select(g => g.ToField(_plan)).Concat(_aggregateExpr.Select(a => a.ToField(_plan))).ToList());

    public List<ILogicalPlan> Children() => new() { _plan };

    public ILogicalPlan Plan => _plan;
    public List<ILogicalExpression> Expr => _groupExpr;
    public List<LogicalAggregateExpression> AggregateExpr => _aggregateExpr;
}