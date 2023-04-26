using CsvRx.Core.Data;
using CsvRx.Data;

namespace CsvRx.Core.Physical.Expressions;

internal interface IPhysicalExpression
{
    internal ColumnDataType GetDataType(Schema schema);

    ColumnValue Evaluate(RecordBatch batch);
}