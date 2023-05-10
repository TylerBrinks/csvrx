using CsvRx.Core.Data;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Core.Physical.Expressions;

namespace CsvRx.Core.Physical.Functions;

internal record SumFunction(IPhysicalExpression InputExpression, string Name, ColumnDataType DataType) 
    : Aggregate(InputExpression), IAggregation
{
    internal override List<QualifiedField> StateFields => new() { QualifiedField.Unqualified($"SUM({Name})", DataType) };

    internal override QualifiedField NamedQualifiedField => QualifiedField.Unqualified(Name, DataType);

    internal override List<IPhysicalExpression> Expressions => new() { Expression };

    public Accumulator CreateAccumulator()
    {
        return new SumAccumulator(DataType);
    }
}