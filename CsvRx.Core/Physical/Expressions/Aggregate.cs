using CsvRx.Core.Data;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Expressions;

internal abstract record Aggregate(IPhysicalExpression Expression) : IPhysicalExpression
{
    public IPhysicalExpression Expression { get; set; } = Expression;

    public virtual ColumnDataType GetDataType(Schema schema)
    {
        throw new NotImplementedException("Aggregates must implement GetDataType");
    }

    public ColumnValue Evaluate(RecordBatch batch, int? schemaIndex = null)
    {
        throw new NotSupportedException();
    }

    internal abstract List<QualifiedField> StateFields { get; }

    internal abstract QualifiedField NamedQualifiedField { get; }

    internal abstract List<IPhysicalExpression> Expressions { get; }
}