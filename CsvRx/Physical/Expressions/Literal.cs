using CsvRx.Data;
using CsvRx.Logical.Expressions;

namespace CsvRx.Physical.Expressions;

internal record Literal(ScalarValue Value) : IPhysicalExpression
{
    public ColumnDataType GetDataType(Schema schema)
    {
        return Value.DataType;
    }
}