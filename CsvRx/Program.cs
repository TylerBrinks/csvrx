using Spectre.Console;

var context = new CsvRx.Core.ExecutionContext();
// ReSharper disable StringLiteralTypo
context.RegisterCsv("aggregate_test_100", @"C:\Users\tyler\source\repos\sink\sqldatafusion\testing\data\csv\aggregate_test_100.csv");
//var results = context.ExecuteSql("SELECT c1, MAX(c3) FROM aggregate_test_100 GROUP BY c1"); //WHERE c11 > .2 AND c11 < 0.9 
//var results = context.ExecuteSql("SELECT * FROM aggregate_test_100");
var results = context.ExecuteSql("SELECT max(c3),c1, min(c3) FROM aggregate_test_100 group by c1");
//var results = context.ExecuteSql("SELECT max(c3) FROM aggregate_test_100");
Console.WriteLine();

Table? table = null;

await foreach (var batch in results)
{
    if(table == null)
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
