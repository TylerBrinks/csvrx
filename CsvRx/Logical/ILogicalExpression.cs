using CsvRx.Data;

namespace CsvRx.Logical;

public interface ILogicalExpression
{
    Field ToField(ILogicalPlan plan);
}