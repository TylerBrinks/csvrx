using System.Collections;
using CsvRx.Core.Data;
using CsvRx.Core.Physical.Expressions;

namespace CsvRx.Core.Execution;

internal record SortExecution(
    List<PhysicalSortExpression> SortExpressions,
    IExecutionPlan Plan) : IExecutionPlan
{
    //TODO: remove this
    public static IExecutionPlan TryNew(List<PhysicalSortExpression> sortExpressions, IExecutionPlan plan)
    {
        return new SortExecution(sortExpressions, plan);
    }

    public async IAsyncEnumerable<RecordBatch> Execute()
    {
        if (SortExpressions.Count == 1)
        {
            yield return await SortSingleColumn();
        }
        else
        {
            yield return await SortColumns();
        }
    }

    private async Task<RecordBatch> SortSingleColumn()
    {
        var collectedBatch = await CoalesceBatches();
        var sortColumn = SortExpressions[0].Expression as Column;

        var array = collectedBatch.Results[sortColumn.Index];
        //TODO sort direction true/false
        var indices = array.GetSortIndices(false);
        collectedBatch.Reorder(indices);
        return collectedBatch;
    }

    private async Task<RecordBatch> SortColumns()
    {
        var sortColumns = SortExpressions.Select(sc => (Column)sc.Expression).ToList();

        var batch = await SortSingleColumn();

        // Skip the first column; it has been sorted already
        var indicesToIgnore = new List<int>
        {
            sortColumns[0].Index
        };
        for (var sortIndex = 1; sortIndex < sortColumns.Count; sortIndex++)
        {
            var previousSortColumn = batch.Results[sortColumns[sortIndex - 1].Index];
            var array = batch.Results[sortColumns[sortIndex].Index];

            // Subsequent sorts are only relevant within the range of the 
            // prior sort values.
            var distinctValueIndices = previousSortColumn.Values.Cast<object>()
                .ToList()
                .Distinct()
                .Select(previousSortColumn.Values.IndexOf)
                .ToArray();

            var groupCount = distinctValueIndices.Length;
            var reorderedIndices = new List<int>();
            for (var j = 0; j < groupCount; j++)
            {
                //0-20
                var start = j == 0 ? 0 : distinctValueIndices[j];
                var end = j == groupCount - 1 ? previousSortColumn.Values.Count : distinctValueIndices[j + 1];
                var take = end - start;

                //TODO sort direction true/false
                var xindices = array.GetSortIndices(false, start, take);
                //var indices= xindices.Select(i => i + distinctValueIndices[j]).ToList();
                var indices= xindices.Select(i => i + start).ToList();
                reorderedIndices.AddRange(indices);
            }

            //TODO sort direction true/false
            batch.Reorder(reorderedIndices, indicesToIgnore);

            indicesToIgnore.Add(sortColumns[sortIndex].Index);
        }

        return batch;
    }

    private async Task<RecordBatch> CoalesceBatches()
    {
        var collectedBatch = new RecordBatch(Plan.Schema);

        await foreach (var batch in Plan.Execute())
        {
            for (var i = 0; i < batch.Results.Count; i++)
            {
                collectedBatch.Results[i].Concat(batch.Results[i].Values);
            }
        }

        return collectedBatch;
    }

    //private ArrayColumnValue EvaluateToSortColumn(PhysicalSortExpression physicalSort, RecordBatch batch)
    //{
    //    return (ArrayColumnValue)physicalSort.Expression.Evaluate(batch);
    //}

    public Schema Schema => Plan.Schema;
}