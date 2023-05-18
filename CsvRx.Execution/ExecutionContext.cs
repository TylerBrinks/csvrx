using CsvRx.Core.Data;
using CsvRx.Core.Execution;
using CsvRx.Core.Logical;
using CsvRx.Core.Physical;
using CsvRx.Csv;
using SqlParser;
using SqlParser.Ast;

namespace CsvRx.Execution;

public class ExecutionContext
{
    private readonly Dictionary<string, DataSource> _tables = new();

    public void RegisterCsv(string tableName, string path)
    {
        RegisterCsv(tableName, path, new CsvOptions());
    }

    public void RegisterCsv(string tableName, string path, CsvOptions options)
    {
        RegisterDataSource(tableName, new CsvDataSource(path, options));
    }

    public void RegisterDataSource(string tableName, DataSource dataSource)
    {
        _tables.Add(tableName, dataSource);
    }

    public async IAsyncEnumerable<RecordBatch> ExecuteSql(string sql)
    {
        await foreach (var batch in ExecuteSql(sql, new QueryOptions()))
        {
            yield return batch;
        }
    }

    public async IAsyncEnumerable<RecordBatch> ExecuteSql(string sql, QueryOptions options)
    {
        var logicalPlan = BuildLogicalPlan(sql);
        var physicalPlan = BuildPhysicalPlan(logicalPlan);

        if (physicalPlan == null)
        {
            throw new InvalidOperationException("Must build a physical plan first");
        }

        await foreach (var batch in physicalPlan.Execute(options))
        {
            yield return batch;
        }
    }
   
    internal ILogicalPlan BuildLogicalPlan(string sql)
    {
        var ast = new Parser().ParseSql(sql);

        if (ast.Count > 1)
        {
            throw new InvalidOperationException("Only 1 SQL statement is supported");
        }

        var plan = ast.First() switch
        {
            Statement.Select s => LogicalPlanner.CreateLogicalPlan(s.Query, _tables),
            _ => throw new NotImplementedException()
        };

        return new LogicalPlanOptimizer().Optimize(plan)!;
    }

    internal static IExecutionPlan BuildPhysicalPlan(ILogicalPlan logicalPlan)
    {
        if (logicalPlan == null)
        {
            throw new InvalidOperationException("Must build a logical plan first");
        }

        return new PhysicalPlanner().CreateInitialPlan(logicalPlan);
    }
}