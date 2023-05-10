namespace CsvRx.Core.Logical.Expressions;

internal record Column(string Name, TableReference? Relation) : ILogicalExpression // = null
{
    public override string ToString()
    {
        return Name;
    }

    internal static Column FromName(string name)
    {
        return new Column(name, null);//
    }
}