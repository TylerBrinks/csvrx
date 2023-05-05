using CsvRx.Core.Data;

namespace CsvRx.Core.Execution;

internal record LimitExecution(IExecutionPlan Plan, int Skip, int Fetch) : IExecutionPlan
{
    public Schema Schema => Plan.Schema;

    public async IAsyncEnumerable<RecordBatch> Execute()
    {
        var skip = Skip;
        var fetch = Fetch;

        var ignoreLimit = Skip == 0 && Fetch == int.MaxValue;

        await foreach (var batch in Plan.Execute())
        {
            if (Fetch == 0)
            {
                continue;
            }

            if (ignoreLimit)
            {
                yield return batch;
            }

            var rowCount = batch.RowCount;

            if (rowCount <= Skip)
            {
                skip -= rowCount;
                continue;
            }

            batch.Slice(skip, rowCount - skip);
            skip = 0;

            if (rowCount < fetch)
            {
                fetch -= rowCount;
                yield return batch;
            }
            else
            {
                //batch.Slice(0, rowCount - fetch);
                batch.Slice(0, fetch);
                fetch = 0;
                yield return batch;
            }
        }
    }
}