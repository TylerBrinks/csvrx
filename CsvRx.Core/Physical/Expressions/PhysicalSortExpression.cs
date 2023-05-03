using CsvRx.Core.Data;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Expressions;

internal record PhysicalSortExpression(IPhysicalExpression Expression, Schema SortSchema, Schema InputSchema) : IPhysicalExpression
{
    public ColumnDataType GetDataType(Schema schema)
    {
        throw new NotImplementedException();
    }

    public ColumnValue Evaluate(RecordBatch batch, int? schemaIndex = null)
    {
        throw new NotImplementedException();
    }
}