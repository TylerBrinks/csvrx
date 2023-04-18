using CsvRx.Data;
using CsvRx.Logical;
using SqlParser.Ast;

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
            Statement.Select s => new Planner().CreateLogicalPlan(s.Query, _tables),
            _ => throw new InvalidOperationException()
        };

        return plan;
    }

    //public List<RecordBatch> Execute(DataFrame df)
    //{
    //    return Execute(df.LogicalPlan);
    //}

    //public List<RecordBatch> Execute(ILogicalPlan plan)
    //{
    //    var optimized = new Optimizer().Optimize(plan);
    //    var physical = new Planner().CreatePhysicalPlan(optimized);

    //    return physical.Execute();
    //}
}