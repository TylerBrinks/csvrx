using CsvRx.Core.Data;
using CsvRx.Data;

namespace CsvRx.Core.Physical.Expressions;

public interface IPhysicalExpression
{
    ColumnDataType GetDataType(Schema schema);

    ColumnValue Evaluate(RecordBatch batch);
}