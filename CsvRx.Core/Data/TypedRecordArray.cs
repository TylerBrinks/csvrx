namespace CsvRx.Core.Data;

public abstract class TypedRecordArray<T> : RecordArray
{
    public List<T> List { get; } = new();
}