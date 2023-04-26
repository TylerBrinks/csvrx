using CsvRx.Core.Data;
using CsvRx.Core.Logical;
using CsvRx.Physical;
using SqlParser.Ast;

namespace CsvRx.Core;

public class ExecutionContext
{
    private readonly Dictionary<string, DataSource> _tables = new();

    public void RegisterCsv(string tableName, string path)
    {
        RegisterCsv(tableName, path, new CsvOptions());
    }

    public void RegisterCsv(string tableName, string path, CsvOptions options)
    {
        Register(tableName, new CsvDataSource(path, options));
    }

    public void Register(string tableName, DataSource df)
    {
        _tables.Add(tableName, df);
    }

    public IEnumerable<RecordBatch> ExecuteSql(string sql)
    {
        var logicalPlan = Sql(sql);
        return ExecutePlan(logicalPlan);
    }

    internal ILogicalPlan Sql(string sql)
    {
        var ast = new Parser().ParseSql(sql);

        if (ast.Count > 1)
        {
            throw new InvalidOperationException();
        }

        var plan = ast.First() switch
        {
            Statement.Select s => new LogicalPlanner().CreateLogicalPlan(s.Query, _tables),
            _ => throw new NotImplementedException()
        };

        return plan;
    }

    internal IEnumerable<RecordBatch> ExecutePlan(ILogicalPlan plan)
    {
        var optimized = OptimizeLogicalPlan(plan);
        return CreatePhysicalPlan(optimized).Execute();
    }

    internal IExecutionPlan CreatePhysicalPlan(ILogicalPlan logicalPlan)
    {
        var optimized = OptimizeLogicalPlan(logicalPlan);

        return new PhysicalPlanner().CreateInitialPlan(optimized);
    }

    internal ILogicalPlan OptimizeLogicalPlan(ILogicalPlan logicalPlan)
    {
        return new LogicalPlanOptimizer().Optimize(logicalPlan);
    }
}