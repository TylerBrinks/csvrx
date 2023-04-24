using CsvRx.Data;

namespace CsvRx.Physical.Expressions;

public interface IPhysicalExpression
{
    ColumnDataType GetDataType(Schema schema);
}

public record AggregateExpression : IPhysicalExpression
{
    public ColumnDataType GetDataType(Schema schema)
    {
        throw new NotImplementedException();
    }

    public virtual List<Field> StateFields { get; } = new();

    public virtual Field Field { get; }
}

public record CountFunction(IPhysicalExpression Expr, string Name, ColumnDataType DataType) : AggregateExpression
{
    public override List<Field> StateFields => new() { new($"{Name}[count]", DataType) };

    public override Field Field => new (Name, DataType);
}

public record SumFunction(IPhysicalExpression Expr, string Name, ColumnDataType DataType) : AggregateExpression
{
    public override List<Field> StateFields => new() { new($"{Name}[sum]", DataType) };
    public override Field Field => new (Name, DataType);
}

public record MinFunction(IPhysicalExpression Expr, string Name, ColumnDataType DataType) : AggregateExpression
{
    public override List<Field> StateFields => new() { new($"{Name}[min]", DataType) };
    public override Field Field => new (Name, DataType);
}

public record MaxFunction(IPhysicalExpression Expr, string Name, ColumnDataType DataType) : AggregateExpression
{
    public override List<Field> StateFields => new () { new ($"{Name}[max]", DataType)};
    public override Field Field => new (Name, DataType);
}