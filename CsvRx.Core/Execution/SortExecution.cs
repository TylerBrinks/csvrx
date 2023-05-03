using CsvRx.Core.Data;
using CsvRx.Core.Physical.Expressions;

namespace CsvRx.Core.Execution;

internal record SortExecution(List<PhysicalSortExpression> SortExpressions, IExecutionPlan Plan) : IExecutionPlan
{
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
        var sortExpression = SortExpressions[0];
        var sortColumn = sortExpression.Expression as Column;

        var array = collectedBatch.Results[sortColumn.Index];

        var indices = array.GetSortIndices(sortExpression.Ascending);
        collectedBatch.Reorder(indices);
        return collectedBatch;
    }

    private async Task<RecordBatch> SortColumns()
    {
        var sortColumns = SortExpressions.Select(sc => (Column)sc.Expression).ToList();
        // The first column will always be sorted in full.  Proactively 
        // sorting here makes it simpler to build an index list of
        // columns that can be skipped in subsequent sorting passes
        var batch = await SortSingleColumn();

        // Skip the first column; it has been sorted already
        var indicesToIgnore = new List<int> { sortColumns[0].Index };

        for (var sortIndex = 1; sortIndex < sortColumns.Count; sortIndex++)
        {
            // Sorting multiple columns must preserve the order of previously 
            // sorted columns.  To do so, the previous column is used to construct
            // a distinct list of values, which is effectively an array-based way
            // of doing a GroupBy operation.  These values represent the boundary
            // for each sub-group that needs to be sorted.
            // For example, if a previous column sort yields values
            // 'a', 'a', 'a', 'b, 'b', 'c', 'c', 'c'
            // The the subsequent sort will sort all items with values related
            // to 'a', then all values related to 'b', and finally 'c.'  This
            // preserves the previous sort order akin to OrderBy().ThenBy()... etc.
            var previousSortColumn = batch.Results[sortColumns[sortIndex - 1].Index];
            var array = batch.Results[sortColumns[sortIndex].Index];

            // Get the index boundaries for the subsequent sort groups
            var distinctValueIndices = previousSortColumn.Values.Cast<object>()
                .ToList()
                .Distinct()
                .Select(previousSortColumn.Values.IndexOf)
                .ToArray();

            var groupCount = distinctValueIndices.Length;

            // Store the sort indices for each sub-group.  They will
            // be applied in one operation instead of multiple passes.
            var reorderedIndices = new List<int>();

            for (var groupIndex = 0; groupIndex < groupCount; groupIndex++)
            {
                // 0 in the case it's the first pass or the whole column
                var start = groupIndex == 0 ? 0 : distinctValueIndices[groupIndex];
                // End boundary or end of list; whichever comes first
                var end = groupIndex == groupCount - 1 ? previousSortColumn.Values.Count : distinctValueIndices[groupIndex + 1];
                var groupSize = end - start;

                var ascending = SortExpressions[sortIndex].Ascending;
                var indices = array.GetSortIndices(ascending, start, groupSize)
                    // Add the boundary offset to each set of indices so 
                    // the full array can be reordered in one operation
                    .Select(i => i + start)
                    .ToList();

                reorderedIndices.AddRange(indices);
            }
            // Reorder all arrays that have not been sorted
            batch.Reorder(reorderedIndices, indicesToIgnore);
            // This column has been sorted; track to prevent re-sorting 
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

    public Schema Schema => Plan.Schema;
}