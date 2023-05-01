namespace CsvRx.Core.Logical.Rules;

internal interface ILogicalPlanOptimizationRule
{
    ApplyOrder ApplyOrder { get; }
    ILogicalPlan? TryOptimize(ILogicalPlan plan);
}