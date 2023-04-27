using CsvRx.Core.Data;
namespace CsvRx.Core.Physical.Expressions;

internal abstract record AggregateExpression(IPhysicalExpression InputExpression) : IPhysicalExpression
{
    public virtual ColumnDataType GetDataType(Schema schema)
    {
        throw new NotImplementedException();
    }

    public abstract ColumnValue Evaluate(RecordBatch batch);

    internal virtual List<Field> StateFields { get; } = new();

    internal abstract Field Field { get; }
}