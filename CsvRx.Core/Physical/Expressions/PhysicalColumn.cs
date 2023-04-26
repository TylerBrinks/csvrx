using CsvRx.Core.Data;
using CsvRx.Data;

namespace CsvRx.Core.Physical.Expressions;

internal record PhysicalColumn(string Name, int Index) : IPhysicalExpression
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