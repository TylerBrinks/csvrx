﻿using CsvRx.Core.Logical;
using CsvRx.Core.Logical.Expressions;

namespace CsvRx.Core.Data;

public class Schema
{
    public Schema(List<QualifiedField> fields)
    {
        Fields = fields;
    }

    //TODO should fields ever be null?
    public List<QualifiedField> Fields { get; }

    public QualifiedField? GetField(string name)
    {
        //return Fields.FirstOrDefault(f => /*f != null &&*/ f.Name == name);
        return FieldsWithUnqualifiedName(name).FirstOrDefault();
    }

    public QualifiedField? GetQualifiedField(TableReference? qualifier, string name)
    {
        return qualifier != null
            ? FieldsWithQualifiedName(qualifier, name).FirstOrDefault()
            : GetField(name);
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
}