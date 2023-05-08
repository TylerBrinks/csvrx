using CsvRx.Core.Data;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Core.Physical.Expressions;

namespace CsvRx.Core.Physical.Functions;

internal record StandardDeviationFunction(
        IPhysicalExpression InputExpression, 
        string Name, 
        ColumnDataType DataType,
        StatisticType StatisticType)
    : Aggregate(InputExpression), IAggregation
{
    internal override List<Field> StateFields => new()
    {
        new($"STDDEV({Name})[count]", ColumnDataType.Integer),
        new($"STDDEV({Name})[mean]", ColumnDataType.Double),
        new($"STDDEV({Name})[m2]",  ColumnDataType.Double),
    };

    internal override Field Field => new(Name, DataType);

    internal override List<IPhysicalExpression> Expressions => new() { Expression };

    public Accumulator CreateAccumulator()
    {
        return new StandardDeviationAccumulator(DataType, StatisticType);
    }
}

internal record VarianceFunction(
        IPhysicalExpression InputExpression,
        string Name,
        ColumnDataType DataType,
        StatisticType StatisticType)
    : Aggregate(InputExpression), IAggregation
{
    internal override List<Field> StateFields => new()
    {
        new($"VAR({Name})[count]", ColumnDataType.Integer),
        new($"VAR({Name})[mean]", ColumnDataType.Double),
        new($"VAR({Name})[m2]",  ColumnDataType.Double),
    };

    internal override Field Field => new(Name, DataType);

    internal override List<IPhysicalExpression> Expressions => new() { Expression };

    public Accumulator CreateAccumulator()
    {
        return new VarianceAccumulator(DataType, StatisticType);
    }
}
