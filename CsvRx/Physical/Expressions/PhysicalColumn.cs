using CsvRx.Data;

namespace CsvRx.Physical.Expressions;

public record PhysicalColumn(string Name, int Index) : IPhysicalExpression
{
    public ColumnDataType GetDataType(Schema schema)
    {
        return schema.Fields[Index].DataType;
    }

    public ColumnValue Evaluate(RecordBatch batch)
    {
        return new ArrayColumnValue(batch.Results[Index].Array, batch.Schema.Fields[Index].DataType);
    }
}