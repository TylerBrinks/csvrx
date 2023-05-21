using CsvRx.Core.Logical;
using CsvRx.Core.Logical.Expressions;

namespace CsvRx.Core.Data;

public abstract record Field(string Name, ColumnDataType DataType)
{
    public override string ToString()
    {
        return $"{Name}:{DataType}";
    }
}

public record QualifiedField(string Name, ColumnDataType DataType, TableReference? Qualifier = null) : Field(Name, DataType)
{
    internal Column QualifiedColumn()
    {
        return new Column(Name, Qualifier);
    }

    internal string QualifiedName
    {
        get
        {
            var qualifier = Qualifier != null ? $"{Qualifier.Name}."  : string.Empty;
            return $"{qualifier}{Name}";
        }
    }

    public static QualifiedField Unqualified(string name, ColumnDataType dataType)
    {
        return new QualifiedField(name, dataType);
    }

    public override string ToString()
    {
        var qualifier = Qualifier != null ? $"{Qualifier}." : "";
        return $"{qualifier}{Name}::{DataType}";
    }

    internal QualifiedField FromQualified(TableReference qualifier)
    {
        return new QualifiedField(Name, DataType, qualifier);
    }
}