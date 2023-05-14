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
    private ILogicalPlan _logicalPlan = null!;

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

    public void BuildLogicalPlan(string sql)
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

        _logicalPlan = new LogicalPlanOptimizer().Optimize(plan)!;
    }

    public IExecutionPlan BuildPhysicalPlan()
    {
        if (_logicalPlan == null)
        {
            throw new InvalidOperationException("Must build a logical plan first");
        }

        return new PhysicalPlanner().CreateInitialPlan(_logicalPlan);
    }

    public async IAsyncEnumerable<RecordBatch> ExecutePlan(IExecutionPlan executionPlan, QueryOptions options)
    {
        if (executionPlan == null)
        {
            throw new InvalidOperationException("Must build a physical plan first");
        }

        await foreach (var batch in ExecuteLogicalPlan(executionPlan, options))
        {
            yield return batch;
        }
    }


    internal static async IAsyncEnumerable<RecordBatch> ExecuteLogicalPlan(IExecutionPlan physicalPlan, QueryOptions options)
    {
        await foreach (var batch in physicalPlan.Execute(options))
        {
            yield return batch;
        }
    }
}