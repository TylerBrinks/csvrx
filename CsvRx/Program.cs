
var context = new CsvRx.Core.ExecutionContext();
// ReSharper disable StringLiteralTypo
context.RegisterCsv("aggregate_test_100", @"C:\Users\tyler\source\repos\sink\sqldatafusion\testing\data\csv\aggregate_test_100.csv");
var batches = context.ExecuteSql("SELECT c1, MAX(c3) FROM aggregate_test_100 GROUP BY c1"); //WHERE c11 > .2 AND c11 < 0.9 
Console.WriteLine();

foreach (var batch in batches)
{
    for (var i = 0; i < batch.RowCount; i++)
    {
        for (var j = 0; j < batch.Results.Count; j++)
        {
            Console.Write(batch.Results[j].Array[i]);
            Console.Write("\t");
        }
        Console.Write("\r\n");
        Console.WriteLine();
    }
}
