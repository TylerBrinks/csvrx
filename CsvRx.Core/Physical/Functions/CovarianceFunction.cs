using CsvRx.Core.Data;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Core.Physical.Expressions;
// ReSharper disable StringLiteralTypo

namespace CsvRx.Core.Physical.Functions;

internal record CovarianceFunction(
        IPhysicalExpression InputExpression1,
        IPhysicalExpression InputExpression2,
        string Name,
        ColumnDataType DataType,
        StatisticType StatisticType)
    : Aggregate(InputExpression1), IAggregation
{
    private string? _prefix;
   
    internal override List<QualifiedField> StateFields => new()
    {
         QualifiedField.Unqualified($"{StatePrefix}({Name})[count]", ColumnDataType.Integer),
         QualifiedField.Unqualified($"{StatePrefix}({Name})[mean1]", ColumnDataType.Double),
         QualifiedField.Unqualified($"{StatePrefix}({Name})[mean2]", ColumnDataType.Double),
         QualifiedField.Unqualified($"{StatePrefix}({Name})[algoConst]", ColumnDataType.Double),
    };

    private string StatePrefix => _prefix ??= StatisticType == StatisticType.Population ? "_POP" : "";
    
    internal override QualifiedField NamedQualifiedField => new(Name, DataType);

    internal override List<IPhysicalExpression> Expressions => new() { InputExpression1, InputExpression2 };

    public Accumulator CreateAccumulator()
    {
        return new CovarianceAccumulator(DataType, StatisticType);
    }
}