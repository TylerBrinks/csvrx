using CsvRx.Data;

namespace CsvRx.Logical;

public abstract record LogicalAggregateExpression(string Name, ILogicalExpression Expression) : ILogicalExpression
{
    public virtual Field ToField(ILogicalPlan plan)
    {
        return new Field(Name); //TODO data type , Expression.ToField(plan).DataType
    }
}