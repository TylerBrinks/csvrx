using CsvRx.Core.Data;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Core.Physical.Expressions;

namespace CsvRx.Core.Physical.Functions;

internal record CountFunction(IPhysicalExpression Expression, string Name, ColumnDataType DataType) 
    : Aggregate(Expression), IAggregation
{
    internal override List<QualifiedField> StateFields => new() { QualifiedField.Unqualified($"COUNT({Name})", DataType) };

    internal override QualifiedField NamedQualifiedField => QualifiedField.Unqualified(Name, DataType);

    internal override List<IPhysicalExpression> Expressions => new() { Expression };

    public Accumulator CreateAccumulator()
    {
        return new CountAccumulator();
    }
}