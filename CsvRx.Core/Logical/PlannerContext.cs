using CsvRx.Core.Data;

namespace CsvRx.Core.Logical;

internal record PlannerContext(Dictionary<string, DataSource> DataSources, List<TableReference> TableReferences)
{
    //TODO outer query schema
    //internal Schema? OuterQuerySchema { get; private set; }

    //internal void SetOuterQuerySchema(Schema outerSchema)
    //{
    //    OuterQuerySchema = outerSchema;
    //}
}