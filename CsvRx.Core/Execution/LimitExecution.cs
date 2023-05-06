using CsvRx.Core.Data;

namespace CsvRx.Core.Execution;

internal record LimitExecution(IExecutionPlan Plan, int Skip, int Fetch) : IExecutionPlan
{
    public Schema Schema => Plan.Schema;

    public async IAsyncEnumerable<RecordBatch> Execute(QueryOptions options)
    {
        var skip = Skip;
        var fetch = Fetch;

        var ignoreLimit = Skip == 0 && Fetch == int.MaxValue;

        await foreach (var batch in Plan.Execute(options))
        {
            if (fetch == 0)
            {
                // Fetch satisfied; short circuit further iteration
                break;
            }

            if (ignoreLimit)
            {
                yield return batch;
            }

            var rowCount = batch.RowCount;

            if (rowCount <= skip)
            {
                skip -= rowCount;
                continue;
            }
            
            batch.Slice(skip, Math.Min(rowCount - skip, fetch));
            
            skip = 0;

            if (rowCount < fetch)
            {
                fetch -= batch.RowCount;
            }
            else
            {
                fetch = 0;
            }

            yield return batch;
        }
    }
}