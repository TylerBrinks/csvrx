using CsvRx.Core.Data;

namespace CsvRx.Core.Logical.Plans;

internal record SubqueryAlias(ILogicalPlan Plan, Schema Schema, string Alias) : ILogicalPlan
{
    public static ILogicalPlan TryNew(ILogicalPlan plan, Schema schema, string alias)
    {
        var tableReference = new TableReference(alias);
        var schemaFields = schema.Fields.Select(f => f.FromQualified(tableReference)).ToList();
        var aliasedSchema = new Schema(schemaFields);

        return new SubqueryAlias(plan, aliasedSchema, alias);
    }

    public string ToStringIndented(Indentation? indentation)
    {
        var indent = indentation ?? new Indentation();
        return $"{this}{indent.Next(Plan)}";
    }

    public override string ToString()
    {
        return $"Table Alias Subquery: {Alias}";
    }
}