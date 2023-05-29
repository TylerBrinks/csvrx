using CsvRx.Core.Data;

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

    private async Task<RecordBatch> ExecuteSingleBatch(string sql)
    {
        var execution = _context.ExecuteSql(sql);

        var enumerator = execution.GetAsyncEnumerator();

        await enumerator.MoveNextAsync();
        return enumerator.Current;
    }

    [Fact]
    public async Task Query_Ignore_Field_Name_Case()
    {
        var batch = await ExecuteSingleBatch("select employee_id, Employee_Id, EMPLOYEE_ID from employees");
        Assert.Equal(107, batch.RowCount);
    }

    [Fact]
    public async Task Query_Counts_Table_Records()
    {
        var batch = await ExecuteSingleBatch("select count(employee_id) from employees");

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
        var batch = await ExecuteSingleBatch(sql);

        Assert.Equal(15, batch.RowCount);

        sql = """
                SELECT count(*)
                FROM employees 
                WHERE salary > 10000
                """;
        batch = await ExecuteSingleBatch(sql);

        Assert.Equal(1, batch.RowCount);
        Assert.Equal(15L, batch.Results[0].Values[0]);
    }

    [Fact]
    public async Task Query_Evaluates_Aliased_Self_Join()
    {
        var sql = """
                SELECT 
                    mgr.employee_id as MgrId, 
                    emp.employee_id, 
                    mgr.first_name MgrFirst, 
                    mgr.last_name MgrLast,
                    emp.first_name EmpFirst, 
                    emp.last_name EmpLast
                FROM employees mgr
                join employees emp
                on mgr.employee_id = emp.manager_id
                where emp.manager_id = 100
                order by emp.manager_id
                """;

        var batch = await ExecuteSingleBatch(sql);

        Assert.Equal(14, batch.RowCount);
    }

    [Fact]
    public async Task Query_Evaluates_Subquery_Records()
    {
        const string sql = """
            SELECT *
            FROM 
                (SELECT 
                    employee_id, 
                    first_name,
                    last_name
                FROM employees)
            """;

        var batch = await ExecuteSingleBatch(sql);

        Assert.Equal(107, batch.RowCount);
        Assert.Equal(3, batch.Results.Count);
    }
}

