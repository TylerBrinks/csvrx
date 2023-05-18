using CsvRx.Core.Data;
using CsvRx.Core.Execution;
using CsvRx.Core.Physical.Expressions;
using SqlParser.Ast;
using Schema = CsvRx.Core.Data.Schema;

namespace CsvRx.Tests.Physical;

public class PlanTests
{
    private Execution.ExecutionContext _context;
    private Schema _schema;
    private InMemoryDataSource _memDb;

    public PlanTests()
    {
        _context = new Execution.ExecutionContext();

        _schema = new Schema(new List<QualifiedField>
        {
            new("a", ColumnDataType.Integer),
            new("b", ColumnDataType.Utf8),
            new("c", ColumnDataType.Double)
        });

        _memDb = new InMemoryDataSource(_schema, new List<int> { 0, 1, 2 });

        _context.RegisterDataSource("db", _memDb);
    }

    [Fact]
    public void Context_Optimizes_Wildcard_Projections()
    {
        const string sql = "select * from db";

        var logicalPlan = _context.BuildLogicalPlan(sql);
        var physicalPlan = Execution.ExecutionContext.BuildPhysicalPlan(logicalPlan);

        Assert.IsType<InMemoryTableExecution>(physicalPlan);
        Assert.Equal(3, physicalPlan.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Optimizes_Select_Projections()
    {
        const string sql = "select a,b,c from db";
        
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var physicalPlan = Execution.ExecutionContext.BuildPhysicalPlan(logicalPlan);

        Assert.IsType<InMemoryTableExecution>(physicalPlan);
        Assert.Equal(3, physicalPlan.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Optimizes_Literal_Filtered_Selects()
    {
        const string sql = "select a,b,c from db where a = '1'";

        var logicalPlan = _context.BuildLogicalPlan(sql);
        var filterExec = (FilterExecution)Execution.ExecutionContext.BuildPhysicalPlan(logicalPlan);
        var binary = (Binary)filterExec.Predicate;

        Assert.Equal(3, filterExec.Schema.Fields.Count);
        Assert.IsType<Column>(binary.Left);
        Assert.IsType<Literal>(binary.Right);
        Assert.Equal(BinaryOperator.Eq, binary.Op);
        Assert.IsType<InMemoryTableExecution>(filterExec.Plan);
        Assert.Equal(3, filterExec.Plan.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Optimizes_Column_Filtered_Selects()
    {
        const string sql = "select a,b,c from db where a > b";

        var logicalPlan = _context.BuildLogicalPlan(sql);
        var filterExec = (FilterExecution)Execution.ExecutionContext.BuildPhysicalPlan(logicalPlan);
        var binary = (Binary)filterExec.Predicate;

        Assert.Equal(3, filterExec.Schema.Fields.Count);
        Assert.IsType<Column>(binary.Left);
        Assert.IsType<Column>(binary.Right);
        Assert.Equal(BinaryOperator.Gt, binary.Op);
        Assert.IsType<InMemoryTableExecution>(filterExec.Plan);
        Assert.Equal(3, filterExec.Plan.Schema.Fields.Count);
    }


    [Fact]
    public void Context_Uses_Multiple_Aggregation_Steps()
    {
        const string sql = "SELECT avg(a) FROM db group by a";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var projectionExec = (ProjectionExecution)Execution.ExecutionContext.BuildPhysicalPlan(logicalPlan);

        Assert.Single(projectionExec.Schema.Fields);
        Assert.Equal("Avg(a)", projectionExec.Expressions[0].Name);
        Assert.Single(projectionExec.Schema.Fields);

        var finalAggregateExec = (AggregateExecution)projectionExec.Plan;
        Assert.Equal(2, finalAggregateExec.Schema.Fields.Count);
        Assert.Equal(AggregationMode.Final, finalAggregateExec.Mode);
        Assert.Equal("a", finalAggregateExec.GroupBy.Expression[0].Name);
        Assert.Single(finalAggregateExec.AggregateExpressions);
        Assert.Equal("a", ((Column)finalAggregateExec.AggregateExpressions[0].Expression).Name);

        var partialAggregateExec = (AggregateExecution)finalAggregateExec.Plan;
        Assert.Equal(2, partialAggregateExec.Schema.Fields.Count);
        Assert.Equal(AggregationMode.Partial, partialAggregateExec.Mode);
        Assert.Equal("a", partialAggregateExec.GroupBy.Expression[0].Name);
        Assert.Single(partialAggregateExec.AggregateExpressions);
        Assert.Equal("a", ((Column)partialAggregateExec.AggregateExpressions[0].Expression).Name);
       
        var scanExec = (InMemoryTableExecution)partialAggregateExec.Plan;
        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Converts_Distinct_To_Aggregates()
    {
        const string sql = "SELECT distinct a FROM db";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var finalAggregateExec = (AggregateExecution)Execution.ExecutionContext.BuildPhysicalPlan(logicalPlan);

        Assert.Single(finalAggregateExec.Schema.Fields);
        Assert.Equal(AggregationMode.Final, finalAggregateExec.Mode);
        Assert.Equal("a", finalAggregateExec.GroupBy.Expression[0].Name);
        Assert.Empty(finalAggregateExec.AggregateExpressions);

        var partialAggregateExec = (AggregateExecution) finalAggregateExec.Plan;
        Assert.Single(partialAggregateExec.Schema.Fields);
        Assert.Equal(AggregationMode.Partial, partialAggregateExec.Mode);
        Assert.Equal("a", partialAggregateExec.GroupBy.Expression[0].Name);
        Assert.Empty(partialAggregateExec.AggregateExpressions);

        var projectionExec = (ProjectionExecution) partialAggregateExec.Plan;
        Assert.Single(projectionExec.Schema.Fields);
        Assert.Equal("a", projectionExec.Expressions[0].Name);
        Assert.Single(projectionExec.Schema.Fields);

        var scanExec = (InMemoryTableExecution) projectionExec.Plan;
        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Orders_Projected_Column()
    {
        const string sql = "SELECT a FROM db order by a";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var sortExec = (SortExecution)Execution.ExecutionContext.BuildPhysicalPlan(logicalPlan);

        Assert.Equal(3, sortExec.Schema.Fields.Count);
        Assert.Equal("a", ((Column)sortExec.SortExpressions[0].Expression).Name);

        var scanExec = (InMemoryTableExecution)sortExec.Plan;
        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Orders_Unused_Column()
    {
        const string sql = "SELECT a FROM db order by b";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var sortExec = (SortExecution)Execution.ExecutionContext.BuildPhysicalPlan(logicalPlan);

        Assert.Equal(3, sortExec.Schema.Fields.Count);
        Assert.Equal("a", ((Column)sortExec.SortExpressions[0].Expression).Name);

        var scanExec = (InMemoryTableExecution)sortExec.Plan;
        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    /*
     * "SELECT c1, MAX(c3) FROM mycsv GROUP BY c1", //WHERE c11 > .2 AND c11 < 0.9 

    "SELECT avg(c3) FROM mycsv group by c1",
    "SELECT c1, c3 FROM mycsv order by c1, c3",

    "SELECT c1 as abc FROM mycsv group by 1",
    "SELECT c1, count(c3) as cnt FROM mycsv group by c1",
    "SELECT covar(c2, c12) aa FROM mycsv",

    "SELECT c1, c2 as abc FROM mycsv where c1 = 'c'",
    "SELECT c1 as a, c3 FROM mycsv order by a limit 23 offset 20",
    "SELECT c1, c2 as abc FROM mycsv mv where mv.c1 = 'c'",


    //****var sql = "SELECT test_a.c2, test_a.c3, test_b.c2 FROM test_a join test_b USING(c1)";
    //select t1.* from t t1 CROSS JOIN t t2"
    //let sql = "SELECT test.col_int32 FROM test JOIN ( SELECT col_int32 FROM test WHERE false ) AS ta1 ON test
    //var sql = "SELECT test_a.c2, test_a.c3, test_b.c2 FROM test_a full outer join test_b on test_a.c1 = test_b.c1";
    //var sql = "SELECT test_a.c2, test_a.c3 FROM test_a left semi join test_b on test_a.c1 = test_b.c1";

    "SELECT ta.c2 aa, ta.c3 bb, tb.c2 tb FROM test_a ta join test_b tb on ta.c1 = tb.c1"
     */
}