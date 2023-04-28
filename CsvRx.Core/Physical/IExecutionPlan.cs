using CsvRx.Core.Data;

namespace CsvRx.Core.Physical;

public interface IExecutionPlan
{
    Schema Schema { get; }
    IAsyncEnumerable<RecordBatch> Execute();
}
