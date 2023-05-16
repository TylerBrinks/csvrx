using CsvRx.Core.Data;
using Spectre.Console;
using SqlParser;

var context = new CsvRx.Execution.ExecutionContext();
// ReSharper disable StringLiteralTypo
context.RegisterCsv("mycsv", @"C:\Users\tyler\source\repos\sink\CsvRx\CsvRx\test_100.csv");
context.RegisterCsv("test_a", @"C:\Users\tyler\source\repos\sink\CsvRx\CsvRx\join_a.csv");
context.RegisterCsv("test_b", @"C:\Users\tyler\source\repos\sink\CsvRx\CsvRx\join_b.csv");

//var results = context.ExecuteSql("SELECT c1, MAX(c3) FROM aggregate_test_100 GROUP BY c1"); //WHERE c11 > .2 AND c11 < 0.9 
//var results = context.ExecuteSql("SELECT * FROM aggregate_test_100");
//var results = context.ExecuteSql("SELECT avg(c3) FROM aggregate_test_100 group by c1");
//var results = context.ExecuteSql("SELECT c1, c3 FROM aggregate_test_100 order by c1, c3");

//var sql = "SELECT c1 as abc FROM mycsv group by 1";
//var sql = "SELECT c1, count(c3) as cnt FROM mycsv group by c1";
//var sql = "SELECT c1, count(c3) as cnt FROM mycsv group by c1 having cnt > 18";
//var sql = "SELECT c1, c2 as abc FROM mycsv where c1 = 'c'";
//var sql = "SELECT c1 as a, c3 FROM mycsv order by a limit 23 offset 20"
//var sql = "SELECT c1, c2 as abc FROM mycsv mv where mv.c1 = 'c'";


var sql = "SELECT test_a.c2, test_a.c3 FROM test_a left join test_b on test_a.c1 = test_b.c1";

try
{
    Console.WriteLine();
    var options = new QueryOptions
    {
        BatchSize = 3
    };
    Table? table = null;
    context.BuildLogicalPlan(sql);
    var exec = context.BuildPhysicalPlan();

    await foreach (var batch in context.ExecutePlan(exec, options))
    {
        if (table == null)
        {
            table = new Table();
            foreach (var field in batch.Schema.Fields)
            {
                table.AddColumn(field.Name);
            }
        }

        for (var i = 0; i < batch.RowCount; i++)
        {
            table.AddRow(batch.Results.Select(value => value.Values[i]?.ToString() ?? "").ToArray());
        }
    }

    AnsiConsole.Write(table);
}
catch (ParserException pe)
{
    NotifyInvalidSyntax(sql, pe.Message, Convert.ToUInt32(pe.Line), Convert.ToUInt32(pe.Column));
}
catch (TokenizeException te)
{
    NotifyInvalidSyntax(sql, te.Message, Convert.ToUInt32(te.Line), Convert.ToUInt32(te.Column));
}
//catch (Exception ex)
//{
//    throw ex;
//}

static void NotifyInvalidSyntax(string query, string message, uint line, uint column)
{
    AnsiConsole.MarkupLine("[red]Invalid SQL Statement[/]");
    AnsiConsole.MarkupLine($"[red]{message}[/]");
    AnsiConsole.WriteLine();
    var lines = query.Split(Environment.NewLine).ToList();
    var pointer = new string('-', (int)column - 1);

    var index = (int)line;

    lines.Insert(index, $"{pointer}^");

    for (var i = 0; i < lines.Count; i++)
    {
        var color = i == index ? "yellow" : "darkseagreen";
        AnsiConsole.MarkupLine($"[{color}]{lines[i]}[/]");
    }
}