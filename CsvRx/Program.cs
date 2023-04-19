using CsvRx.Logical;
using ExecutionContext = CsvRx.ExecutionContext;

var context = new ExecutionContext();
// ReSharper disable StringLiteralTypo
context.RegisterCsv("aggregate_test_100", @"C:\Users\tyler\source\repos\sink\sqldatafusion\testing\data\csv\aggregate_test_100.csv");
var df = context.Sql("SELECT c1, MAX(c3) FROM aggregate_test_100 WHERE c11 > 0.1 AND c11 < 0.9 GROUP BY c1");
//var df = context.Sql("SELECT MAX(c3) FROM aggregate_test_100");
//context.Execute(df);
Console.Write(df.ToStringIndented(new Indentation()));