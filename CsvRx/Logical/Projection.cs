using CsvRx.Data;

namespace CsvRx.Logical;

public class Projection : ILogicalPlan
{
    private readonly ILogicalPlan _plan;
    private readonly List<ILogicalExpression> _expr;

    public Projection(ILogicalPlan plan, List<ILogicalExpression> expr)
    {
        _plan = plan;
        _expr = expr;
    }

    public Schema Schema => new(_expr.Select(s => s.ToField(_plan)).ToList());

    public List<ILogicalPlan> Children() => new() { _plan };

    public ILogicalPlan Plan => _plan;
    public List<ILogicalExpression> Expr => _expr;
}