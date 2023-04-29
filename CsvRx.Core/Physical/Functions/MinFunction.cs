﻿using CsvRx.Core.Data;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Core.Physical.Expressions;

namespace CsvRx.Core.Physical.Functions;

internal record MinFunction(IPhysicalExpression InputExpression, string Name, ColumnDataType DataType) 
    : Aggregate(InputExpression), IAggregation
{
    internal override List<Field> StateFields => new() { new($"{Name}[min]", DataType) };
    
    internal override Field Field => new(Name, DataType);

    internal override List<IPhysicalExpression> Expressions => new() {InputExpression};

    public Accumulator CreateAccumulator()
    {
        return new MinAccumulator();
    }
}