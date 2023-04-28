using CsvRx.Core.Data;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Core.Physical.Expressions;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Functions;

internal record MinFunction(IPhysicalExpression InputExpression, string Name, ColumnDataType DataType) 
    : AggregateExpression(InputExpression), IAggregation
{
    internal override List<Field> StateFields => new() { new($"{Name}[min]", DataType) };
    internal override Field Field => new(Name, DataType);

    public override ColumnValue Evaluate(RecordBatch batch)
    {
        throw new NotImplementedException();
    }

    public Accumulator CreateAccumulator()
    {
        return new MinAccumulator();
    }
}