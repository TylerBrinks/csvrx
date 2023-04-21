using CsvRx.Logical;
using CsvRx.Physical;

namespace CsvRx.Data;

public record DataFrame(SessionState State, ILogicalPlan Plan)
{
    public IExecutionPlan CreatePhysicalPlan()
    {
        return State.CreatePhysicalPlan(Plan);
    }
}
