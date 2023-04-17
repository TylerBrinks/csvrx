using CsvRx.Data;

namespace CsvRx.Logical;

public abstract record MathExpression
    (string Name, string Op, ILogicalExpression Left, ILogicalExpression Right) : ILogicalExpression
{
    public Field ToField(ILogicalPlan plan)
    {
        return new Field(Name);// left.ToField(plan).dataType
    }
}