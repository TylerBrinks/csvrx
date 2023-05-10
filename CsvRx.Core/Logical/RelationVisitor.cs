using SqlParser.Ast;

namespace CsvRx.Core.Logical;

internal class RelationVisitor : Visitor
{
    public override ControlFlow PostVisitRelation(TableFactor relation)
    {
        if (relation is TableFactor.Table table)
        {
            string? alias = null;

            if (table.Alias != null)
            {
                alias = table.Alias.Name;
            }

            var reference = new TableReference(table.Name, alias);
            if (!TableReferences.Contains(reference))
            {
                TableReferences.Add(reference);
            }
        }

        return ControlFlow.Continue;
    }

    internal List<TableReference> TableReferences { get; } = new();
}