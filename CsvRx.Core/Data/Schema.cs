using CsvRx.Core.Logical;
using CsvRx.Core.Logical.Expressions;

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
        //return Fields.Where(f => f.Qualifier != null && f.Qualifier.Name == qualifier.Name && f.Name == columnName);
        return Fields.Where(f => f.Qualifier != null && 
                                 f.Qualifier.Name.Equals(qualifier.Name, StringComparison.InvariantCultureIgnoreCase) &&
                                 f.Name.Equals(columnName, StringComparison.InvariantCultureIgnoreCase));
            // == qualifier.Name && f.Name == columnName);
    }

    public IEnumerable<QualifiedField> FieldsWithUnqualifiedName(string columnName)
    {
        //return Fields.Where(f => f.Name == columnName);
        return Fields.Where(f => f.Name.Equals(columnName, StringComparison.InvariantCultureIgnoreCase));
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

    internal QualifiedField? FieldWithQualifiedName(TableReference qualifier, string columnName)
    {
        var index = IndexOfColumnByName(qualifier, columnName);

        return index.HasValue ? Fields[index!.Value] : null;
    }

    private int? IndexOfColumnByName(TableReference? qualifier, string name)
    {
        var matches = Fields.Where(field =>
        {
            if (qualifier != null && field.Qualifier != null)
            {
                return field.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase) &&
                       field.Qualifier.Name.Equals(qualifier.Name, StringComparison.InvariantCultureIgnoreCase);
                //return field.Name == name && field.Qualifier.Name == qualifier.Name;
            }

            if (qualifier != null && field.Qualifier == null)
            {
                var column = Column.FromQualifiedName(field.Name);
                
                if (column.Relation != null && column.Relation == qualifier)
                {
                    return column.Relation.Name.Equals(qualifier.Name, StringComparison.InvariantCultureIgnoreCase)
                           && column.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase);
                    //return column.Relation.Name == qualifier.Name && column.Name == name;
                }

                return false;
            }

            return field.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase);
            //== name;
        }).ToList();

        if (!matches.Any())
        {
            return null;
        }

        return Fields.IndexOf(matches.First());
    }

    internal QualifiedField? FieldWithUnqualifiedName(string name)
    {
        var matches = FieldsWithUnqualifiedName(name).ToList();

        return matches.Count switch
        {
            0 => null,//throw new InvalidOperationException("Unqualified field not found"),

            1 => matches.First(),

            _ => FindField()
        };

        QualifiedField FindField()
        {
            var fieldsWithoutQualifier = matches.Where(f => f.Qualifier == null).ToList();

            if (fieldsWithoutQualifier.Count == 1)
            {
                return fieldsWithoutQualifier[0];
            }

            throw new InvalidOperationException("Unqualified field not found");
        }
    }

    internal QualifiedField? FieldWithName(Column column)
    {
        return column.Relation != null
            ? FieldWithQualifiedName(column.Relation, column.Name)
            : FieldWithUnqualifiedName(column.Name);
    }
}