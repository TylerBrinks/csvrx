using CsvRx.Core.Logical;
using CsvRx.Core.Logical.Expressions;
using static SqlParser.Ast.FetchDirection;

namespace CsvRx.Core.Data;

public class Schema
{
    private readonly bool _fullyQualified;

    public Schema(List<QualifiedField> fields)
    {
        Fields = fields;
        _fullyQualified = fields.Any(f => f.Qualifier != null);
    }

    public List<QualifiedField> Fields { get; }

    public QualifiedField? GetField(string name)
    {
        return FieldsWithUnqualifiedName(name).FirstOrDefault();
    }

    internal QualifiedField? GetFieldFromColumn(Column column)
    {
        // Fields may not be qualified for a given schema, so
        // method for looking up fields depends on the type
        // of schema being queried.
        return _fullyQualified && column.Relation != null
            ? FieldsWithQualifiedName(column.Relation, column.Name).FirstOrDefault()
            : GetField(column.Name);
    }

    internal int? IndexOfColumn(Column column)
    {
        var field = GetFieldFromColumn(column);

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

        foreach (var field in Fields)
        {
            hash.Add(field);
        }

        return hash.ToHashCode();
    }

    public IEnumerable<QualifiedField> FieldsWithQualifiedName(TableReference qualifier, string columnName)
    {
        return Fields.Where(f => f.Qualifier != null && f.Qualifier.Name == qualifier.Name && f.Name == columnName);
    }

    public IEnumerable<QualifiedField> FieldsWithUnqualifiedName(string columnName)
    {
        return Fields.Where(f => f.Name == columnName);
    }

    public Schema Join(Schema joinSchema)
    {
        var fields = Fields.ToList().Concat(joinSchema.Fields.ToList()).ToList();
        return new Schema(fields);
    }

    internal bool HasColumn(Column column)
    {
        return GetFieldFromColumn(column) != null;
    }

    //public bool IsColumnFromSchema(Column column)
    //{
    //    throw new NotImplementedException();
    //}
}