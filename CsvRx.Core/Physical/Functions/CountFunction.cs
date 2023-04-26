﻿using CsvRx.Core.Data;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Core.Physical.Expressions;
using CsvRx.Data;

namespace CsvRx.Core.Physical.Functions;

internal record CountFunction(IPhysicalExpression InputExpression, string Name, ColumnDataType DataType) : AggregateExpression(InputExpression)
{
    internal override List<Field> StateFields => new() { new($"{Name}[count]", DataType) };

    internal override Field Field => new(Name, DataType);

    public override ColumnValue Evaluate(RecordBatch batch)
    {
        throw new NotImplementedException();
    }

    internal override Accumulator CreateAccumulator()
    {
        return new CountAccumulator();
    }
}