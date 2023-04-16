using CsvRx.Data;

namespace CsvRx.Logical;

public record Column(string Name) : ILogicalExpression
{
    public Field ToField(ILogicalPlan plan)
    {
        return plan.Schema.Fields.First(f => f.Name == Name);
    }
}