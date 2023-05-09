﻿using CsvRx.Core.Data;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Core.Physical.Expressions;

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
   
    internal override List<Field> StateFields => new()
    {
        new($"COVAR{StatePrefix}({Name})[count]", ColumnDataType.Integer),
        new($"COVAR{StatePrefix}({Name})[mean1]", ColumnDataType.Double),
        new($"COVAR{StatePrefix}({Name})[mean2]", ColumnDataType.Double),
        new($"COVAR{StatePrefix}({Name})[algoConst]", ColumnDataType.Double),
    };

    private string StatePrefix => _prefix ??= StatisticType == StatisticType.Population ? "_POP" : "";
    
    internal override Field Field => new(Name, DataType);

    internal override List<IPhysicalExpression> Expressions => new() { InputExpression1, InputExpression2 };

    public Accumulator CreateAccumulator()
    {
        return new CovarianceAccumulator(DataType, StatisticType);
    }
}