using CsvRx.Logical;
using ExecutionContext = CsvRx.ExecutionContext;

var context = new ExecutionContext();
// ReSharper disable StringLiteralTypo
context.RegisterCsv("aggregate_test_100", @"C:\Users\tyler\source\repos\sink\sqldatafusion\testing\data\csv\aggregate_test_100.csv");
var plan = context.Sql("SELECT c1, MAX(c3) FROM aggregate_test_100 WHERE c11 > 1 AND c11 < 0.9 GROUP BY c1");
//var plan = context.Sql("SELECT MAX(c3) FROM aggregate_test_100");
//var plan = context.Sql("SELECT c1 FROM aggregate_test_100");

context.ExecutePlan(plan);
