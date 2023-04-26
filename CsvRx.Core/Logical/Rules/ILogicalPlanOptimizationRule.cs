namespace CsvRx.Core.Logical.Rules;

public interface ILogicalPlanOptimizationRule
{
    ApplyOrder ApplyOrder { get; }
    ILogicalPlan TryOptimize(ILogicalPlan plan);
}