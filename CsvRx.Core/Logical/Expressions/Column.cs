namespace CsvRx.Core.Logical.Expressions;

internal record Column(string Name, TableReference? Relation) : ILogicalExpression
{
    public override string ToString()
    {
        var relation = Relation != null ? Relation.Alias ?? Relation.Name + "." : string.Empty;
        return $"{relation}{Name}";
    }

    internal static Column FromName(string name)
    {
        return new Column(name, null);
    }

    public string FlatName => Relation != null ? $"{Relation.Name}.{Name}" : Name;
}