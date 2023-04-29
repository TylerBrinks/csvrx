using CsvRx.Core.Execution;

namespace CsvRx.Core.Data;

public abstract class DataSource
{
    public abstract Schema? Schema { get; }

    public abstract IExecutionPlan Scan(List<int> projection);
}