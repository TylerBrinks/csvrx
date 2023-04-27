using Spectre.Console;

var context = new CsvRx.Core.ExecutionContext();
// ReSharper disable StringLiteralTypo
context.RegisterCsv("aggregate_test_100", @"C:\Users\tyler\source\repos\sink\sqldatafusion\testing\data\csv\aggregate_test_100.csv");
var batches = context.ExecuteSql("SELECT c1, MAX(c3) FROM aggregate_test_100 GROUP BY c1"); //WHERE c11 > .2 AND c11 < 0.9 
Console.WriteLine();


var schema = batches.First().Schema;

var table = new Table();
foreach (var field in schema.Fields)
{
    table.AddColumn(field.Name);
}

foreach (var batch in batches)
{
    for (var i = 0; i < batch.RowCount; i++)
    {
        table.AddRow(batch.Results.Select(value => value.Values[i]?.ToString() ?? "").ToArray());
    }
}
AnsiConsole.Write(table);
