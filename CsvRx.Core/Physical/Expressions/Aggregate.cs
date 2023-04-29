using CsvRx.Core.Data;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Expressions;

internal abstract record Aggregate(IPhysicalExpression InputExpression) : IPhysicalExpression
{
    public virtual ColumnDataType GetDataType(Schema schema)
    {
        throw new NotImplementedException();
    }

    public ColumnValue Evaluate(RecordBatch batch)
    {
        throw new NotSupportedException();
    }

    internal abstract List<Field> StateFields { get; }

    internal abstract Field Field { get; }

    internal abstract List<IPhysicalExpression> Expressions { get; }
}