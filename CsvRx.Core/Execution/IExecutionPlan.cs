using CsvRx.Core.Data;

namespace CsvRx.Core.Execution;

public interface IExecutionPlan
{
    Schema Schema { get; }
    IAsyncEnumerable<RecordBatch> Execute(QueryOptions options);
}
