using CsvRx.Data;

namespace CsvRx.Physical.Expressions;

public record PhysicalColumn(string Name, int Index) : IPhysicalExpression
{
    public ColumnDataType GetDataType(Schema schema)
    {
        return schema.Fields[Index].DataType;
    }
}