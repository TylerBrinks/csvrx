using CsvRx.Core.Data;
using Schema = CsvRx.Core.Data.Schema;

namespace CsvRx.Tests.Physical;

public class ExecutionTests
{
    private readonly Execution.ExecutionContext _context;

    public ExecutionTests()
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

    //[Fact]
    //public void Context_Executes_Async_Batches()
    //{
    //    const string sql = """
    //        select db.a, avg(joinDb.c3)
    //        from db 
    //        join joinDb on joinDb.c = db.a
    //        group by db.a
    //        """;

    //    var logicalPlan = _context.BuildLogicalPlan(sql);
    //    var execution = Execution.ExecutionContext.BuildPhysicalPlan(logicalPlan);
    //}
}