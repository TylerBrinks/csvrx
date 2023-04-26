using CsvRx.Core.Data;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Core.Physical.Expressions;
using CsvRx.Data;

namespace CsvRx.Core.Physical.Functions;

public record CountFunction(IPhysicalExpression InputExpression, string Name, ColumnDataType DataType) : AggregateExpression(InputExpression)
{
    public override List<Field> StateFields => new() { new($"{Name}[count]", DataType) };

    public override Field Field => new(Name, DataType);

    public override ColumnValue Evaluate(RecordBatch batch)
    {
        throw new NotImplementedException();
    }

    public override Accumulator CreateAccumulator()
    {
        return new CountAccumulator();
    }
}