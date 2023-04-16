using CsvRx.Data;

namespace CsvRx.Physical;

public interface IPhysicalPlan
{
    Schema Schema { get; }
    List<RecordBatch> Execute();
    List<IPhysicalPlan> Children { get; }
}