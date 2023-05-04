﻿using Spectre.Console;
using SqlParser;

var context = new CsvRx.Execution.ExecutionContext();
// ReSharper disable StringLiteralTypo
context.RegisterCsv("aggregate_test_100", @"C:\Users\tyler\source\repos\sink\sqldatafusion\testing\data\csv\aggregate_test_100.csv");
//var results = context.ExecuteSql("SELECT c1, MAX(c3) FROM aggregate_test_100 GROUP BY c1"); //WHERE c11 > .2 AND c11 < 0.9 
//var results = context.ExecuteSql("SELECT * FROM aggregate_test_100");
//var results = context.ExecuteSql("SELECT avg(c3) FROM aggregate_test_100 group by c1");
//var results = context.ExecuteSql("SELECT c1, c3 FROM aggregate_test_100 order by c1, c3");

//var sql = "SELECT distinct s s s c1 as abc FROM aggregate_test_1003";
var sql = """
    SELECT distinct c1 as abc
    FROM aggregate_test_1003
    """;

try
{
    var results = context.ExecuteSql(sql);

    Console.WriteLine();

    Table? table = null;

    await foreach (var batch in results)
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
catch (Exception ex)
{
    throw ex;
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
        var color = i == index ? "yellow" : "teal";
        AnsiConsole.MarkupLine($"[{color}]{lines[i]}[/]");
    }
}