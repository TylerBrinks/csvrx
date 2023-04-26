using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Data;

namespace CsvRx.Core.Physical.Expressions;

internal record Literal(ScalarValue Value) : IPhysicalExpression
{
    public ColumnDataType GetDataType(Schema schema)
    {
        return Value.DataType;
    }

    public ColumnValue Evaluate(RecordBatch batch)
    {
        return new ScalarColumnValue(Value, batch.RowCount, Value.DataType);
    }
}