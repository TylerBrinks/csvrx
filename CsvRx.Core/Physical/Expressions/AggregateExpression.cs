using CsvRx.Core.Data;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Data;

namespace CsvRx.Core.Physical.Expressions;

internal abstract record AggregateExpression(IPhysicalExpression InputExpression) : IPhysicalExpression
{
    public ColumnDataType GetDataType(Schema schema)
    {
        throw new NotImplementedException();
    }

    public abstract ColumnValue Evaluate(RecordBatch batch);

    internal virtual List<Field> StateFields { get; } = new();

    internal virtual Field Field { get; }

    internal abstract Accumulator CreateAccumulator();
}