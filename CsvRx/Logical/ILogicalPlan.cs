using CsvRx.Data;

namespace CsvRx.Logical;

public interface ILogicalPlan
{
    Schema Schema { get; }

    List<ILogicalPlan> Children();
}