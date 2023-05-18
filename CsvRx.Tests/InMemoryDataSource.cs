using CsvRx.Core.Data;
using CsvRx.Core.Execution;
using CsvRx.Core.Logical.Plans;
namespace CsvRx.Tests;

public class InMemoryDataSource : DataSource
{
    public InMemoryDataSource(Schema schema, ICollection<int> projection)
    {
        var fields = schema.Fields.Where((_, i) => projection.Contains(i)).ToList();
        Schema = new Schema(fields);
    }

    public override Schema Schema { get; }

    public override IExecutionPlan Scan(List<int> projection)
    {
        return new InMemoryTableExecution(Schema, projection);
    }
}