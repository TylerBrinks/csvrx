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

    internal override List<QualifiedField> StateFields => new()
    {
        QualifiedField.Unqualified($"VAR{StatePrefix}({Name})[count]", ColumnDataType.Integer),
        QualifiedField.Unqualified($"VAR{StatePrefix}({Name})[mean]", ColumnDataType.Double),
        QualifiedField.Unqualified($"VAR{StatePrefix}({Name})[m2]",  ColumnDataType.Double),
    };

    private string StatePrefix => _prefix ??= StatisticType == StatisticType.Population ? "_POP" : "";

    internal override QualifiedField NamedQualifiedField => new(Name, DataType);

    internal override List<IPhysicalExpression> Expressions => new() { Expression };

    public Accumulator CreateAccumulator()
    {
        return new VarianceAccumulator(DataType, StatisticType);
    }
}