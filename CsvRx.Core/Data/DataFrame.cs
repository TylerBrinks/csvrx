using CsvRx.Core.Logical;
using CsvRx.Physical;

namespace CsvRx.Core.Data;

internal record DataFrame(SessionState State, ILogicalPlan Plan)
{
    public IExecutionPlan CreatePhysicalPlan()
    {
        return State.CreatePhysicalPlan(Plan);
    }
}
