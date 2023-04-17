using CsvRx.Data;

namespace CsvRx.Logical;

public class Selection : ILogicalPlan
{
    private readonly ILogicalPlan _plan;
    private readonly ILogicalExpression _expr;

    public Selection(ILogicalPlan plan, ILogicalExpression expr)
    {
        _plan = plan;
        _expr = expr;
    }

    public Schema Schema => _plan.Schema;

    public List<ILogicalPlan> Children() => new() { _plan };

    public ILogicalPlan Plan => _plan;
    public ILogicalExpression Expr => _expr;
}