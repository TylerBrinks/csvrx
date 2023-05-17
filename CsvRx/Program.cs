using CsvRx.Core.Data;
using Spectre.Console;
using SqlParser;

var context = new CsvRx.Execution.ExecutionContext();
// ReSharper disable StringLiteralTypo
context.RegisterCsv("mycsv", @"C:\Users\tyler\source\repos\sink\CsvRx\CsvRx\test_100.csv");
context.RegisterCsv("test_a", @"C:\Users\tyler\source\repos\sink\CsvRx\CsvRx\join_a.csv");
context.RegisterCsv("test_b", @"C:\Users\tyler\source\repos\sink\CsvRx\CsvRx\join_b.csv");

var queries = new List<string>
{
    "SELECT c1, MAX(c3) FROM mycsv GROUP BY c1", //WHERE c11 > .2 AND c11 < 0.9 
    "SELECT * FROM mycsv",
    "SELECT avg(c3) FROM mycsv group by c1",
    "SELECT c1, c3 FROM mycsv order by c1, c3",

    "SELECT c1 as abc FROM mycsv group by 1",
    "SELECT c1, count(c3) as cnt FROM mycsv group by c1",
    "SELECT covar(c2, c12) aa FROM mycsv",

    "SELECT c1, c2 as abc FROM mycsv where c1 = 'c'",
    "SELECT c1 as a, c3 FROM mycsv order by a limit 23 offset 20",
    "SELECT c1, c2 as abc FROM mycsv mv where mv.c1 = 'c'",


    //****var sql = "SELECT test_a.c2, test_a.c3, test_b.c2 FROM test_a join test_b USING(c1)";
    //select t1.* from t t1 CROSS JOIN t t2"
    //let sql = "SELECT test.col_int32 FROM test JOIN ( SELECT col_int32 FROM test WHERE false ) AS ta1 ON test
    //var sql = "SELECT test_a.c2, test_a.c3, test_b.c2 FROM test_a full outer join test_b on test_a.c1 = test_b.c1";
    //var sql = "SELECT test_a.c2, test_a.c3 FROM test_a left semi join test_b on test_a.c1 = test_b.c1";

    "SELECT ta.c2 aa, ta.c3 bb, tb.c2 tb FROM test_a ta join test_b tb on ta.c1 = tb.c1"

};

foreach (var sql in queries)
{
    try
    {
        //Console.Clear();
        Console.WriteLine();
        Console.WriteLine();
        AnsiConsole.MarkupLine($"[green]{sql}[/]");

        var options = new QueryOptions
        {
            BatchSize = 3
        };

        Table? table = null;

        await foreach (var batch in context.ExecuteSql(sql, options))
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
    Console.ReadKey();
}

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