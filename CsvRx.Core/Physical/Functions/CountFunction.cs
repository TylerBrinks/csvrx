using CsvRx.Core.Data;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Core.Physical.Expressions;

namespace CsvRx.Core.Physical.Functions;

internal record CountFunction(IPhysicalExpression Expression, string Name, ColumnDataType DataType) 
    : Aggregate(Expression), IAggregation
{
    internal override List<Field> StateFields => new() { new($"{Name}[count]", DataType) };

    internal override Field Field => new(Name, DataType);

    internal override List<IPhysicalExpression> Expressions => new() { Expression };

    public Accumulator CreateAccumulator()
    {
        return new CountAccumulator();
    }
}