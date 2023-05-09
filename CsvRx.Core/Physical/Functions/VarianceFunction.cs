using CsvRx.Core.Data;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Core.Physical.Expressions;

namespace CsvRx.Core.Physical.Functions;

internal record VarianceFunction(
        IPhysicalExpression InputExpression,
        string Name,
        ColumnDataType DataType,
        StatisticType StatisticType)
    : Aggregate(InputExpression), IAggregation
{
    private string? _prefix;

    internal override List<Field> StateFields => new()
    {
        new($"VAR{StatePrefix}({Name})[count]", ColumnDataType.Integer),
        new($"VAR{StatePrefix}({Name})[mean]", ColumnDataType.Double),
        new($"VAR{StatePrefix}({Name})[m2]",  ColumnDataType.Double),
    };

    private string StatePrefix => _prefix ??= StatisticType == Core.StatisticType.Population ? "_POP" : "";

    internal override Field Field => new(Name, DataType);

    internal override List<IPhysicalExpression> Expressions => new() { Expression };

    public Accumulator CreateAccumulator()
    {
        return new VarianceAccumulator(DataType, StatisticType);
    }
}