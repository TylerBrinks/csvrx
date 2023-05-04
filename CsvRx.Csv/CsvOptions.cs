namespace CsvRx.Csv; 

public class CsvOptions
{
    public string Delimiter { get; set; } = ",";
    public bool HasHeader { get; set; } = true;
    public int InferMax { get; set; } = 100;
    public int ReadBatchSize { get; set; } = 64;
}