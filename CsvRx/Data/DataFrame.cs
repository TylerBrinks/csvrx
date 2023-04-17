////using CsvRx.Logical;

//using CsvRx.Logical;

//namespace CsvRx.Data;

//public class DataFrame
//{
//    public DataFrame()
//    {
        
//    }
//    public DataFrame(ILogicalPlan plan)
//    {
//        LogicalPlan = plan;
//    }

//    public ILogicalPlan LogicalPlan { get; }

//    public Schema Schema => LogicalPlan.Schema;

//    //public DataFrame Project(List<ILogicalExpression> expr)
//    //{
//    //    return new DataFrame(new Projection(LogicalPlan, expr));
//    //}

//    //public DataFrame Filter(ILogicalExpression expr)
//    //{
//    //    return new DataFrame(new Selection(LogicalPlan, expr));
//    //}

//    //public DataFrame Aggregate(List<ILogicalExpression> groupBy, List<LogicalAggregateExpression> aggregateExpr)
//    //{
//    //    return new DataFrame(new Aggregate(LogicalPlan, groupBy, aggregateExpr));
//    //}
//}