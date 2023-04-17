using CsvRx.Data;

namespace CsvRx.Physical;

public record ScanExec(DataSource DataSource, List<string> Projection) : IPhysicalPlan
{
    public Schema Schema => DataSource.Schema.Select(Projection);
    public List<RecordBatch> Execute()
    {
        return DataSource.Scan(Projection);
    }

    public List<IPhysicalPlan> Children => new List<IPhysicalPlan>();
}