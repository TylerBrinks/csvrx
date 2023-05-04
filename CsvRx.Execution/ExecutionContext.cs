using CsvRx.Core.Data;
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

    public void RegisterDataSource(string tableName, DataSource df)
    {
        _tables.Add(tableName, df);
    }

    public async IAsyncEnumerable<RecordBatch> ExecuteSql(string sql)
    {
        var logicalPlan = BuildLogicalPlan(sql);
        await foreach (var batch in ExecuteLogicalPlan(logicalPlan))
        {
            yield return batch;
        }
    }

    internal ILogicalPlan BuildLogicalPlan(string sql)
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

    internal static async IAsyncEnumerable<RecordBatch> ExecuteLogicalPlan(ILogicalPlan logicalPlan)
    {
        var optimized = new LogicalPlanOptimizer().Optimize(logicalPlan);

        var physicalPlan = new PhysicalPlanner().CreateInitialPlan(optimized);

        await foreach (var batch in physicalPlan.Execute())
        {
            yield return batch;
        }
    }
}