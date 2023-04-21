using CsvRx.Logical.Expressions;

namespace CsvRx.Data;

public class Schema
{
    public Schema(List<Field> fields)
    {
        Fields = fields;
    }

    private Schema()
    {
    }

    public List<Field> Fields { get; } = new();

    public Field? GetField(string name)
    {
        return Fields.FirstOrDefault(f => f.Name == name);
    }

    public int? IndexOfColumn(Column col)
    {
        var field = GetField(col.Name);
        if (field == null)
        {
            return null;

        }
        return Fields.IndexOf(field);
    }

    public Schema Merge(Schema schema)
    {
        foreach (var field in schema.Fields.Where(field => Fields.All(f => f.Name != field.Name)))
        {
            Fields.Add(field);
        }

        return this;
    }

    public static Schema Empty()
    {
        return new Schema();
    }
}