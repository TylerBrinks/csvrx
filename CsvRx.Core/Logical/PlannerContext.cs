using CsvRx.Core.Data;

namespace CsvRx.Core.Logical;

internal record PlannerContext(
    IReadOnlyDictionary<string, DataSource> DataSources,
    List<TableReference> TableReferences)
{
    //TODO outer query schema
    //internal Schema? OuterQuerySchema { get; private set; }

    //internal void SetOuterQuerySchema(Schema outerSchema)
    //{
    //    OuterQuerySchema = outerSchema;
    //}
}