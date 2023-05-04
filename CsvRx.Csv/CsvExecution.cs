using CsvRx.Core.Data;
using CsvRx.Core.Execution;

namespace CsvRx.Csv;

internal class CsvExecution : IExecutionPlan
{
    private readonly List<int> _projection;
    private readonly CsvDataSource _dataSource;

    public CsvExecution(Schema schema, List<int> projection, CsvDataSource dataSource)
    {
        var fields = schema.Fields.Where((_, i) => projection.Contains(i)).ToList();
        Schema = new Schema(fields);
        _projection = projection;
        _dataSource = dataSource;
    }

    public Schema Schema { get; }

    public async IAsyncEnumerable<RecordBatch> Execute()
    {
        await foreach (var slice in _dataSource.Read(_projection))
        {
            var batch = new RecordBatch(Schema);

            foreach (var line in slice)
            {
                for (var i = 0; i < line.Length; i++)
                {
                    batch.Results[i].Add(line[i]);
                }
            }

            yield return batch;
        }
    }
}