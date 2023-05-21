using CsvRx.Core.Data;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Core.Physical.Expressions;

namespace CsvRx.Core.Physical.Functions;

internal record MedianFunction(IPhysicalExpression InputExpression, string Name, ColumnDataType DataType)
    : Aggregate(InputExpression), IAggregation
{
    internal override List<QualifiedField> StateFields => new()
    {
        QualifiedField.Unqualified($"{Name}", DataType)
    };

    internal override QualifiedField NamedQualifiedField => new(Name, DataType);

    internal override List<IPhysicalExpression> Expressions => new() { Expression };

    public Accumulator CreateAccumulator()
    {
        return new MedianAccumulator(DataType);
    }
}