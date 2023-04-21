//namespace CsvRx.Logical;

//internal class LogicalPlanAnalyzer
//{
//    public LogicalPlanAnalyzer()
//    {
//        Rules = new List<IAnalyzeRule>
//        {
//            new InlineTableScan(),
//            new TypeCoercion(),
//            new CountWildcardRule()
//        };
//    }

//    private List<IAnalyzeRule> Rules { get; }

//    public ILogicalPlan Check(ILogicalPlan logicalPlan)
//    {
//        var plan = logicalPlan;

//        foreach (var rule in Rules)
//        {
//            plan = rule.Analyze(plan);
//        }

//        return plan;
//    }
//}