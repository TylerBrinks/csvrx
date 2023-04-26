using CsvRx.Core.Logical.Expressions;
using CsvRx.Data;

namespace CsvRx.Core.Data;

public class Schema
{
    public Schema(List<Field> fields)
    {
        Fields = fields;
    }

    public List<Field> Fields { get; }

    public Field? GetField(string name)
    {
        return Fields.FirstOrDefault(f => f.Name == name);
    }

    internal int? IndexOfColumn(Column col)
    {
        var field = GetField(col.Name);
        if (field == null)
        {
            return null;

        }
        return Fields.IndexOf(field);
    }

    public override bool Equals(object? obj)
    {
        return Equals(obj as Schema);
    }

    public bool Equals(Schema? other)
    {
        if (other == null)
        {
            return false;
        }

        return Fields.SequenceEqual(other.Fields);
    }
}