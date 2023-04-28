using CsvRx.Core.Data;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Expressions;

internal record PhysicalColumn(string Name, int Index) : IPhysicalExpression
{
    public ColumnDataType GetDataType(Schema schema)
    {
        return schema.Fields[Index].DataType;
    }

    public ColumnValue Evaluate(RecordBatch batch)
    {
        return new ArrayColumnValue(batch.Results[Index].Values, batch.Schema.Fields[Index].DataType);
    }
}