namespace CsvRx.Core.Logical.Expressions;

internal record Column(string Name, TableReference? Relation = null) : ILogicalExpression
{
    public override string ToString()
    {
        var relation = Relation != null ? Relation.Alias ?? Relation.Name + "." : string.Empty;
        return $"{relation}{Name}";
    }

    public string FlatName => Relation != null ? $"{Relation.Name}.{Name}" : Name;

    public virtual bool Equals(Column? other)
    {
        if (other == null)
        {
            return false;
        }

        var equal = Name == other.Name;

        if(equal && Relation != null)
        {
            equal &= Relation == other.Relation;
        }

        return equal;
    }

    public static Column FromQualifiedName(string name)
    {
        return new Column(name);
    }
}