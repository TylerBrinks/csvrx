using CsvRx.Core.Logical;
using CsvRx.Physical;

namespace CsvRx;

internal class SessionState
{
    public IExecutionPlan CreatePhysicalPlan(ILogicalPlan logicalPlan)
    {
        var optimized = OptimizeLogicalPlan(logicalPlan);

        return new PhysicalPlanner().CreateInitialPlan(optimized);
    }

    public ILogicalPlan OptimizeLogicalPlan(ILogicalPlan logicalPlan)
    {
        logicalPlan = new LogicalPlanOptimizer().Optimize(logicalPlan);

        Console.Write(logicalPlan.ToStringIndented(new Indentation()));
        return logicalPlan;
    }
}