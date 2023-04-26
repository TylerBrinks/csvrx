
using CsvRx.Core.Data;
using CsvRx.Data;

namespace CsvRx.Physical;

public interface IExecutionPlan
{
    Schema Schema { get; }
    IEnumerable<RecordBatch> Execute();
}
