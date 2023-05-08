using CsvRx.Core.Logical.Expressions;

namespace CsvRx.Core.Data;

public class Schema
{
    public Schema(List<Field> fields)
    {
        Fields = fields!;
    }

    //TODO should fields ever be null?
    public List<Field> Fields { get; }

    public Field? GetField(string name)
    {
        return Fields.FirstOrDefault(f => /*f != null &&*/ f.Name == name);
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
        return other != null && Fields.SequenceEqual(other.Fields);
    }

    public override int GetHashCode()
    {
        HashCode hash = new();

        foreach (var field in Fields/*.Where(f => f!=null)*/)
        {
            hash.Add(field);
        }
      
        return hash.ToHashCode();
    }
}