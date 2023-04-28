using CsvRx.Core.Data;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Expressions;

internal abstract record Aggregate(IPhysicalExpression InputExpression) : IPhysicalExpression
{
    public virtual ColumnDataType GetDataType(Schema schema)
    {
        throw new NotImplementedException();
    }

    public abstract ColumnValue Evaluate(RecordBatch batch);

    internal abstract List<Field> StateFields { get; }

    internal abstract Field Field { get; }
}