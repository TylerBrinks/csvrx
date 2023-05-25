namespace CsvRx.Tests.Integration;

public class QueryTests
{
    private readonly Execution.ExecutionContext _context;


    public QueryTests()
    {
        _context = new Execution.ExecutionContext();
        var root = Directory.GetCurrentDirectory().TrimEnd('\\', '/');
        _context.RegisterCsv("departments", $"{root}/Integration/db_departments.csv");
        _context.RegisterCsv("employees", $"{root}/Integration/db_employees.csv");
        _context.RegisterCsv("jobs", $"{root}/Integration/db_jobs.csv");
        _context.RegisterCsv("locations", $"{root}/Integration/db_locations.csv");
    }

    [Fact]
    public async Task Query_Counts_Table_Records()
    {
        var execution = _context.ExecuteSql("select count(EMPLOYEE_ID) from employees");

        var enumerator = execution.GetAsyncEnumerator();

        await enumerator.MoveNextAsync();
        var batch = enumerator.Current;

        Assert.Equal(1, batch.RowCount);
        Assert.Equal(107L, batch.Results[0].Values[0]);
    }

    [Fact]
    public async Task Query_Evaluates_Numeric_Filters()
    {
        var sql = """
                SELECT first_name, last_name
                FROM employees 
                WHERE salary > 10000
                """;
        var execution = _context.ExecuteSql(sql);

        var enumerator = execution.GetAsyncEnumerator();

        await enumerator.MoveNextAsync();
        var batch = enumerator.Current;

        Assert.Equal(15, batch.RowCount);

        sql = """
                SELECT count(*)
                FROM employees 
                WHERE salary > 10000
                """;
        execution = _context.ExecuteSql(sql);

        enumerator = execution.GetAsyncEnumerator();

        await enumerator.MoveNextAsync();
        batch = enumerator.Current;

        Assert.Equal(1, batch.RowCount);
        Assert.Equal(15L, batch.Results[0].Values[0]);
    }

    //[Fact]
    //public async Task Query_Evaluates_Subquery_Records()
    //{
    //    const string sql = """
    //        SELECT first_name, last_name 
    //        FROM employees 
    //        WHERE salary > 
    //        (SELECT salary  
    //        FROM employees 
    //        WHERE employee_id=163
    //        )
    //        """;
    //    var execution = _context.ExecuteSql(sql);

    //    var enumerator = execution.GetAsyncEnumerator();

    //    await enumerator.MoveNextAsync();
    //    var batch = enumerator.Current;

    //    Assert.Equal(1, batch.RowCount);
    //    Assert.Equal(107L, batch.Results[0].Values[0]);
    //}
}

