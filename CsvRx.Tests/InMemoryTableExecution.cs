using CsvRx.Core.Data;
using CsvRx.Core.Execution;

namespace CsvRx.Tests;

public class InMemoryTableExecution : IExecutionPlan
{
    private readonly Random _random = new();
    private readonly List<int> _projection;

    public InMemoryTableExecution(Schema schema, List<int> projection)
    {
        Schema = schema;
        _projection = projection;
    }

    public Schema Schema { get; }


    public async IAsyncEnumerable<RecordBatch> Execute(QueryOptions options)
    {
        await foreach (var slice in Read(_projection, options.BatchSize))
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

    public async IAsyncEnumerable<List<string?[]>> Read(List<int> indices, int batchSize)
    {
        var data = new List<string?[]>(batchSize);

        for (var i = 0; i < batchSize; i++)
        {
            var line = new string?[indices.Count];

            for (var j = 0; j < indices.Count; j++)
            {
                var value = _random.Next(-100, 100);
                line[j] = value.ToString();
            }

            data.Add(line);
        }

        yield return await Task.FromResult(data);
    }
}