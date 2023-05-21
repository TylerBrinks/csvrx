using CsvRx.Core.Data;

namespace CsvRx.Core.Logical.Plans;

internal record SubqueryAlias(ILogicalPlan Plan, Schema Schema, string Alias) : ILogicalPlanParent
{
    public static ILogicalPlan TryNew(ILogicalPlan plan, string alias)
    {
        var tableReference = new TableReference(alias);
        var schemaFields = plan.Schema.Fields.Select(f => f.FromQualified(tableReference)).ToList();
        var aliasedSchema = new Schema(schemaFields);

        return new SubqueryAlias(plan, aliasedSchema, alias);
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        return $"{this}{indent.Next(Plan)}";
    }

    public override string ToString()
    {
        return $"Subquery Alias: {Alias}";
    }
}