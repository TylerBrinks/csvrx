﻿using CsvRx.Core.Data;
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
    private string? _prefix;

    internal override List<Field> StateFields => new()
    {
        new($"STDDEV{StatePrefix}({Name})[count]", ColumnDataType.Integer),
        new($"STDDEV{StatePrefix}({Name})[mean]", ColumnDataType.Double),
        new($"STDDEV{StatePrefix}({Name})[m2]",  ColumnDataType.Double),
    };

    private string StatePrefix => _prefix ??= StatisticType == Core.StatisticType.Population ? "_POP" : "";

    internal override Field Field => new(Name, DataType);

    internal override List<IPhysicalExpression> Expressions => new() { Expression };

    public Accumulator CreateAccumulator()
    {
        return new StandardDeviationAccumulator(DataType, StatisticType);
    }
}