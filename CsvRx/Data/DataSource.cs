namespace CsvRx.Data;

public abstract class DataSource
{
    public abstract Schema Schema { get; }

    //public virtual List<RecordBatch> Scan(List<string> projection)
    //{
    //    throw new NotImplementedException();
    //}
}