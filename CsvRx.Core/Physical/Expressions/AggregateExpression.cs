using CsvRx.Core.Data;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Data;

namespace CsvRx.Core.Physical.Expressions;

public abstract record AggregateExpression(IPhysicalExpression InputExpression) : IPhysicalExpression
{
    public ColumnDataType GetDataType(Schema schema)
    {
        throw new NotImplementedException();
    }

    public abstract ColumnValue Evaluate(RecordBatch batch);

    public virtual List<Field> StateFields { get; } = new();

    public virtual Field Field { get; }

    public abstract Accumulator CreateAccumulator();
}