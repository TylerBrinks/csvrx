using CsvRx.Core.Data;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Expressions;

internal abstract record Aggregate(IPhysicalExpression InputExpression) : IPhysicalExpression
{
    public IPhysicalExpression InputExpression { get; set; } = InputExpression;

    public virtual ColumnDataType GetDataType(Schema schema)
    {
        throw new NotImplementedException();
    }

    public ColumnValue Evaluate(RecordBatch batch, int? schemaIndex = null)
    {
        throw new NotSupportedException();
    }

    internal abstract List<Field> StateFields { get; }

    internal abstract Field Field { get; }

    internal abstract List<IPhysicalExpression> Expressions { get; }

    //internal virtual void SetAggregateFieldIndex(int index)
    //{
    //    InputExpression =  new Column(Field.Name, index);
    //}
}