using CsvRx.Core.Data;

namespace CsvRx.Core.Logical;

internal record PlannerContext(
    IReadOnlyDictionary<string, DataSource> DataSources,
    List<TableReference> TableReferences
);