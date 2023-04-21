using CsvRx.Data;
using CsvRx.Logical;
using SqlParser.Ast;

namespace CsvRx;

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

    public ILogicalPlan Sql(string sql)
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

    public void ExecutePlan(ILogicalPlan plan)
    {
        var df = new DataFrame(new SessionState(), plan);
        var execution = df.CreatePhysicalPlan();
        
    }
}

internal class InlineTableScan : IAnalyzeRule
{
    public ILogicalPlan Analyze(ILogicalPlan plan)
    {
        return plan.Transform(plan, AnalyzeInternal);
    }

    private ILogicalPlan AnalyzeInternal(ILogicalPlan plan)
    {
        return plan;
    }
}
internal class TypeCoercion : IAnalyzeRule
{
    public ILogicalPlan Analyze(ILogicalPlan plan)
    {
        return plan;
    }
}

internal class CountWildcardRule : IAnalyzeRule
{
    public ILogicalPlan Analyze(ILogicalPlan plan)
    {
        return plan;
    }
}

internal interface IAnalyzeRule
{
    ILogicalPlan Analyze(ILogicalPlan plan);
}