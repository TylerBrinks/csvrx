using CsvRx.Core.Data;
using CsvRx.Physical;

namespace CsvRx.Data;

public abstract class DataSource
{
    public abstract Schema Schema { get; }

    public abstract IExecutionPlan Scan(List<int> projection);
}