using CsvRx.Data;
using CsvRx.Logical.Expressions;

namespace CsvRx.Physical.Expressions;

internal record Literal(ScalarValue Value) : IPhysicalExpression
{
    public ColumnDataType GetDataType(Schema schema)
    {
        return Value.DataType;
    }

    public ColumnValue Evaluate(RecordBatch batch)
    {
        return new ScalarColumnValue(Value, batch.Results.First().Array.Count, Value.DataType);
    }
}