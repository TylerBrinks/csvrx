using System.Collections;
using CsvRx.Core.Data;
using CsvRx.Core.Physical.Joins;
using CsvRx.Core.Values;

namespace CsvRx.Core.Execution;

internal class JoinMap : Dictionary<int, List<int>> { }

internal record JoinLeftData(JoinMap JoinMap, RecordBatch Batch);


internal abstract record JoinExecution
{
    protected (long[], long[]) ApplyJoinFilterToIndices(
        RecordBatch buildInputBuffer,
        RecordBatch probeBatch,
        long[] buildIndices,
        long[] probeIndices,
        JoinFilter filter,
        JoinSide buildSide)
    {
        if (!buildIndices.Any() && !probeIndices.Any())
        {
            return (buildIndices, probeIndices);
        }

        var intermediateBatch = BuildBatchFromIndices(
            filter!.Schema,
            buildInputBuffer,
            probeBatch,
            buildIndices.AsNullable(),
            probeIndices.AsNullable(),
            filter.ColumnIndices,
            buildSide);

        var mask = ((BooleanColumnValue)filter!.FilterExpression.Evaluate(intermediateBatch)).Values;

        var leftFiltered = buildIndices.Where((_, index) => mask[index]).ToArray();
        var rightFiltered = probeIndices.Where((_, index) => mask[index]).ToArray();

        return (leftFiltered, rightFiltered);
    }

    protected static RecordBatch BuildBatchFromIndices(
        Schema schema,
        RecordBatch buildInputBuffer,
        RecordBatch probeBatch,
        IReadOnlyCollection<long?> buildIndices,
        IReadOnlyCollection<long?> probeIndices,
        List<ColumnIndex> columnIndices,
        JoinSide buildSide)
    {
        if (!schema.Fields.Any())
        {
            return new RecordBatch(schema);
        }

        var columns = new List<IList>();

        foreach (var columnIndex in columnIndices)
        {
            IList array;

            if (columnIndex.JoinSide == buildSide)
            {
                var recordArray = buildInputBuffer.Results[columnIndex.Index];
                if (recordArray.Values.Count == 0 || buildIndices.NullCount() == buildIndices.Count)
                {
                    array = recordArray.NewEmpty(buildIndices.Count).Values;
                }
                else
                {
                    array = buildIndices.Select(i => i == null ? null : recordArray.Values[(int)i]).ToList();
                }
            }
            else
            {
                var recordArray = probeBatch.Results[columnIndex.Index];
                if (recordArray.Values.Count == 0 || probeIndices.NullCount() == probeIndices.Count)
                {
                    array = recordArray.NewEmpty(probeIndices.Count).Values;
                }
                else
                {
                    array = probeIndices.Select(i => i == null ? null : recordArray.Values[(int)i]).ToList();
                }
            }

            columns.Add(array);
        }

        return RecordBatch.TryNewWithLists(schema, columns);
    }


    internal static (long?[] finalLeft, long?[] finalRight) GetFinalIndices(bool[] mask, JoinType joinType)
    {
        long[] leftIndices;

        if (joinType == JoinType.LeftSemi)
        {
            leftIndices = Enumerable.Range(0, mask.Length)
                .Select(index => ((long)index, mask[index]))
                .Where(i => i.Item2)
                .Select(i => i.Item1)
                .ToArray();
        }
        else
        {
            // Left, LeftAnti, and Full
            leftIndices = Enumerable.Range(0, mask.Length)
                .Select(index => ((long)index, mask[index]))
                .Where(i => !i.Item2)
                .Select(i => i.Item1)
                .ToArray();
        }

        return (leftIndices.AsNullable(), new long?[leftIndices.Length]);
    }
    /// <summary>
    /// Appends unmatched left index values to the list of matched
    /// left index values and fills the right index list with null 
    /// values to keep the length of both index lists consistent.
    /// </summary>
    /// <param name="leftIndices">Left side index array</param>
    /// <param name="rightIndices">Right side index array</param>
    /// <param name="leftUnmatchedIndices">Unlatched left side index list</param>
    /// <returns></returns>
    internal static (long?[] LeftIndices, long?[] RightIndices) AppendLeftIndices(
        long[] leftIndices, long[] rightIndices, IReadOnlyCollection<long> leftUnmatchedIndices)
    {
        var unmatchedSize = leftUnmatchedIndices.Count;
        if (unmatchedSize == 0)
        {
            return (leftIndices.AsNullable(), rightIndices.AsNullable());
        }

        var newLeftIndices = leftIndices.Concat(leftUnmatchedIndices).AsNullable();
        var newRightIndices = rightIndices.AsNullable().Concat(new long?[unmatchedSize]).ToArray();

        return (newLeftIndices, newRightIndices);
    }
    /// <summary>
    /// Appends unmatched right index values to the list of matched
    /// right index values and fills the left index list with null 
    /// values to keep the length of both index lists consistent.
    /// </summary>
    /// <param name="leftIndices">Left side index array</param>
    /// <param name="rightIndices">Right side index array</param>
    /// <param name="rightUnmatchedIndices">Unlatched right side index list</param>
    /// <returns></returns>
    internal static (long?[] LeftIndices, long?[] RightIndices) AppendRightIndices(
        long[] leftIndices, long[] rightIndices, IReadOnlyCollection<long> rightUnmatchedIndices)
    {
        var unmatchedSize = rightUnmatchedIndices.Count;
        if (unmatchedSize == 0)
        {
            return (leftIndices.AsNullable(), rightIndices.AsNullable());
        }

        var newLeftIndices = leftIndices.AsNullable().Concat(new long?[unmatchedSize]).ToArray();
        var newRightIndices = rightIndices.Concat(rightUnmatchedIndices).AsNullable();

        return (newLeftIndices, newRightIndices);
    }
}