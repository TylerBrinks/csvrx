
using CsvRx.Data;

namespace CsvRx.Physical;

public interface IExecutionPlan
{
    Schema Schema { get; }
}
