﻿using CsvRx.Core;
using CsvRx.Core.Data;
using CsvRx.Core.Execution;
using CsvRx.Core.Physical.Expressions;
using SqlParser.Ast;
using Schema = CsvRx.Core.Data.Schema;

namespace CsvRx.Tests.Physical;

public class PlanTests
{
    private readonly Execution.ExecutionContext _context;

    public PlanTests()
    {
        _context = new Execution.ExecutionContext();

        var schema = new Schema(new List<QualifiedField>
        {
            new("a", ColumnDataType.Integer),
            new("b", ColumnDataType.Utf8),
            new("c", ColumnDataType.Double)
        });
        var memDb = new InMemoryDataSource(schema, new List<int> { 0, 1, 2 });
        _context.RegisterDataSource("db", memDb);

        var joinSchema = new Schema(new List<QualifiedField>
        {
            new("c", ColumnDataType.Integer),
            new("d", ColumnDataType.Utf8),
            new("e", ColumnDataType.Double)
        });
        var joinDb = new InMemoryDataSource(joinSchema, new List<int> { 0, 1, 2 });
        _context.RegisterDataSource("joinDb", joinDb);
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
        var projectionExec = (ProjectionExecution)Execution.ExecutionContext.BuildPhysicalPlan(logicalPlan);

        Assert.Single(projectionExec.Schema.Fields);
        Assert.Equal("a", ((Column)projectionExec.Expressions[0].Expression).Name);

        var sortExec = (SortExecution) projectionExec.Plan;
        Assert.Equal(2, sortExec.Schema.Fields.Count);
        Assert.Single(sortExec.SortExpressions);
        Assert.Equal("b", ((Column)sortExec.SortExpressions[0].Expression).Name);

        var nestedProjection = (ProjectionExecution) sortExec.Plan;
        Assert.Equal(2, nestedProjection.Schema.Fields.Count);
        Assert.Equal("a", ((Column)nestedProjection.Expressions[0].Expression).Name);
        Assert.Equal("b", ((Column)nestedProjection.Expressions[1].Expression).Name);

        var scanExec = (InMemoryTableExecution)nestedProjection.Plan;
        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Groups_Columns()
    {
        const string sql = "SELECT max(a), b FROM db group by b";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var projectionExec = (ProjectionExecution)Execution.ExecutionContext.BuildPhysicalPlan(logicalPlan);
        var finalAggregateExec = (AggregateExecution)projectionExec.Plan;
        var partialAggregateExec = (AggregateExecution)finalAggregateExec.Plan;
        var scanExec = (InMemoryTableExecution)partialAggregateExec.Plan;

        Assert.Equal(2, projectionExec.Schema.Fields.Count);
        Assert.Equal("MAX(db.a)", ((Column)projectionExec.Expressions[0].Expression).Name);
        Assert.Equal("b", ((Column)projectionExec.Expressions[1].Expression).Name);

        Assert.Equal(2, finalAggregateExec.Schema.Fields.Count);
        Assert.Single(finalAggregateExec.AggregateExpressions);
        Assert.Single(finalAggregateExec.GroupBy.Expression);
        Assert.Equal("b", ((Column)finalAggregateExec.GroupBy.Expression[0].Expression).Name);
        Assert.Equal("a", ((Column)finalAggregateExec.AggregateExpressions[0].Expression).Name);

        Assert.Equal(2, partialAggregateExec.Schema.Fields.Count);
        Assert.Single(partialAggregateExec.AggregateExpressions);
        Assert.Single(partialAggregateExec.GroupBy.Expression);
        Assert.Equal("b", ((Column)partialAggregateExec.GroupBy.Expression[0].Expression).Name);
        Assert.Equal("a", ((Column)partialAggregateExec.AggregateExpressions[0].Expression).Name);

        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Replaces_Having_Step_With_Filter()
    {
        const string sql = "SELECT max(a), b FROM db group by b having b = 1";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var projectionExec = (ProjectionExecution)Execution.ExecutionContext.BuildPhysicalPlan(logicalPlan);
        var filterExec = (FilterExecution) projectionExec.Plan;
        var finalAggregateExec = (AggregateExecution)filterExec.Plan;
        var partialAggregateExec = (AggregateExecution)finalAggregateExec.Plan;
        var scanExec = (InMemoryTableExecution)partialAggregateExec.Plan;

        Assert.Equal(2, projectionExec.Schema.Fields.Count);
        Assert.Equal("MAX(db.a)", ((Column)projectionExec.Expressions[0].Expression).Name);
        Assert.Equal("b", ((Column)projectionExec.Expressions[1].Expression).Name);

        Assert.Equal(2, filterExec.Schema.Fields.Count);
        Assert.IsType<Binary>(filterExec.Predicate);

        Assert.Equal(2, finalAggregateExec.Schema.Fields.Count);
        Assert.Single(finalAggregateExec.AggregateExpressions);
        Assert.Single(finalAggregateExec.GroupBy.Expression);
        Assert.Equal("b", ((Column)finalAggregateExec.GroupBy.Expression[0].Expression).Name);
        Assert.Equal("a", ((Column)finalAggregateExec.AggregateExpressions[0].Expression).Name);

        Assert.Equal(2, partialAggregateExec.Schema.Fields.Count);
        Assert.Single(partialAggregateExec.AggregateExpressions);
        Assert.Single(partialAggregateExec.GroupBy.Expression);
        Assert.Equal("b", ((Column)partialAggregateExec.GroupBy.Expression[0].Expression).Name);
        Assert.Equal("a", ((Column)partialAggregateExec.AggregateExpressions[0].Expression).Name);

        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Filters_With_Where_Clause()
    {
        const string sql = "SELECT max(a) as a, b FROM db where b = 'x' group by b";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var projectionExec = (ProjectionExecution)Execution.ExecutionContext.BuildPhysicalPlan(logicalPlan);
        var finalAggregateExec = (AggregateExecution)projectionExec.Plan;
        var partialAggregateExec = (AggregateExecution)finalAggregateExec.Plan;
        var filterExec = (FilterExecution)partialAggregateExec.Plan;
        var subProjection = (ProjectionExecution)filterExec.Plan;
        var scanExec = (InMemoryTableExecution)subProjection.Plan;

        Assert.Equal(2, projectionExec.Schema.Fields.Count);
        Assert.Equal("MAX(db.a)", ((Column)projectionExec.Expressions[0].Expression).Name);
        Assert.Equal("b", ((Column)projectionExec.Expressions[1].Expression).Name);

        Assert.Equal(2, finalAggregateExec.Schema.Fields.Count);
        Assert.Single(finalAggregateExec.AggregateExpressions);
        Assert.Single(finalAggregateExec.GroupBy.Expression);
        Assert.Equal("b", ((Column)finalAggregateExec.GroupBy.Expression[0].Expression).Name);
        Assert.Equal("a", ((Column)finalAggregateExec.AggregateExpressions[0].Expression).Name);

        Assert.Equal(2, partialAggregateExec.Schema.Fields.Count);
        Assert.Single(partialAggregateExec.AggregateExpressions);
        Assert.Single(partialAggregateExec.GroupBy.Expression);
        Assert.Equal("b", ((Column)partialAggregateExec.GroupBy.Expression[0].Expression).Name);
        Assert.Equal("a", ((Column)partialAggregateExec.AggregateExpressions[0].Expression).Name);

        Assert.Equal(2, filterExec.Schema.Fields.Count);
        Assert.IsType<Binary>(filterExec.Predicate);

        Assert.Equal(2, projectionExec.Schema.Fields.Count);
        Assert.Equal("MAX(db.a)", ((Column)projectionExec.Expressions[0].Expression).Name);
        Assert.Equal("b", ((Column)projectionExec.Expressions[1].Expression).Name);

        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Orders_And_Limits_Results()
    {
        const string sql = "SELECT a, b FROM db order by a desc  offset 5 limit 10";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var limitExec = (LimitExecution)Execution.ExecutionContext.BuildPhysicalPlan(logicalPlan);
        var sortExec = (SortExecution) limitExec.Plan;
        var scanExec = (InMemoryTableExecution)sortExec.Plan;

        Assert.Equal(2, logicalPlan.Schema.Fields.Count);
        Assert.Equal(10, limitExec.Fetch);
        Assert.Equal(5, limitExec.Skip);

        Assert.Equal(3, sortExec.Schema.Fields.Count);
        Assert.IsType<PhysicalSortExpression>(sortExec.SortExpressions[0]);

        Assert.Equal(3, scanExec.Schema.Fields.Count);
    }

    [Fact]
    public void Context_Joins_Tables_Results()
    {
        const string sql = "SELECT db.a, joinDb.d FROM db join joinDb on db.c = joinDb.e";
        var logicalPlan = _context.BuildLogicalPlan(sql);
        var projectionExec = (ProjectionExecution)Execution.ExecutionContext.BuildPhysicalPlan(logicalPlan);
        var joinExec = (NestedLoopJoinExecution)projectionExec.Plan;
        var scanExecLeft = (InMemoryTableExecution)joinExec.Left;
        var scanExecRight = (InMemoryTableExecution)joinExec.Right;

        Assert.Equal(2, logicalPlan.Schema.Fields.Count);
        Assert.Equal(2, projectionExec.Expressions.Count);
        
        Assert.Equal(6, joinExec.Schema.Fields.Count);
        Assert.Equal(JoinType.Inner, joinExec.JoinType);

        var binary = (Binary)joinExec.Filter!.FilterExpression;
        Assert.Equal("c", ((Column)binary.Left).Name);
        Assert.Equal("e", ((Column)binary.Right).Name);

        Assert.Equal(3, scanExecLeft.Schema.Fields.Count);
        Assert.Equal(3, scanExecRight.Schema.Fields.Count);
    }
}