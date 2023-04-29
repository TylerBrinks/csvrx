using CsvRx.Core.Data;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Expressions;

internal record Column(string Name, int Index) : IPhysicalExpression
{
    public ColumnDataType GetDataType(Schema schema)
    {
        return schema.Fields[Index].DataType;
    }

    public ColumnValue Evaluate(RecordBatch batch, int? schemaIndex = null)
    {
        return new ArrayColumnValue(batch.Results[schemaIndex ?? Index].Values, batch.Schema.Fields[schemaIndex ?? Index].DataType);
    }
}