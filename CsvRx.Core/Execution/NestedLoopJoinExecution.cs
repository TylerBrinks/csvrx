using CsvRx.Core.Data;
using CsvRx.Core.Physical.Joins;
using System.Collections;
using CsvRx.Core.Values;

namespace CsvRx.Core.Execution;

internal record NestedLoopJoinExecution(
    IExecutionPlan Left,
    IExecutionPlan Right,
    JoinFilter? Filter,
    JoinType JoinType,
    List<ColumnIndex> ColumnIndices,
    Schema Schema) : IExecutionPlan
{
    internal bool LeftIsBuildSide => JoinType is JoinType.Right
        or JoinType.RightSemi
        or JoinType.RightAnti
        or JoinType.Full;

    public async IAsyncEnumerable<RecordBatch> Execute(QueryOptions options)
    {
        if (LeftIsBuildSide)
        {
            // Left side builds require a single left batch
            var leftMerged = new RecordBatch(Left.Schema);

            await foreach (var leftBatch in Left.Execute(options))
            {
                leftMerged.Concat(leftBatch);
            }

            // Bitmap for a full join
            var visitedLeftSide = JoinType == JoinType.Full 
                ? new bool[leftMerged.RowCount] 
                : Array.Empty<bool>();

            await foreach (var rightBatch in Right.Execute(options))
            {
                var intermediate = JointLeftAndRightBatch(leftMerged, rightBatch, ColumnIndices, visitedLeftSide);
                yield return intermediate;
            }

            if (JoinType != JoinType.Full)
            {
                yield break;
            }

            var (finalLeft, finalRight) = GetFinalIndices(visitedLeftSide, JoinType.Full);

            var emptyBatch = new RecordBatch(Left.Schema);
            var finalBatch = BuildBatchFromIndices(Schema, leftMerged, emptyBatch, finalLeft, finalRight, ColumnIndices, JoinSide.Left);
            yield return finalBatch;
        }
        else
        {
            await foreach (var rightBatch in Right.Execute(options))
            {
                await foreach (var leftBatch in Left.Execute(options))
                {
                    yield return JointLeftAndRightBatch(leftBatch, rightBatch, ColumnIndices, Array.Empty<bool>());
                }
            }
        }
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

    private RecordBatch JointLeftAndRightBatch(RecordBatch leftBatch, RecordBatch rightBatch, List<ColumnIndex> columnIndices, bool[] visitedLeftSide)
    {
        var indicesResult = Enumerable.Range(0, leftBatch.RowCount).ToList()
            .Select(leftRowIndex => BuildJoinIndices(leftRowIndex, rightBatch, leftBatch)).ToList();

        var leftIndices = new List<long>();
        var rightIndices = new List<long>();

        foreach (var (leftIndicesResult, rightIndicesResult) in indicesResult)
        {
            leftIndices.AddRange(leftIndicesResult);
            rightIndices.AddRange(rightIndicesResult);
        }

        if (JoinType == JoinType.Full)
        {
            foreach (var leftIndex in leftIndices)
            {
                visitedLeftSide[leftIndex] = true;
            }
        }

        var (leftSide, rightSide) = AdjustIndicesByJoinType(leftIndices.ToArray(), rightIndices.ToArray(), leftBatch.RowCount, rightBatch.RowCount);

        return BuildBatchFromIndices(Schema, leftBatch, rightBatch, leftSide, rightSide, columnIndices, JoinSide.Left);
    }

    private (long[], long[]) BuildJoinIndices(int leftRowIndex, RecordBatch rightBatch, RecordBatch leftBatch)
    {
        var rightRowCount = rightBatch.RowCount;
        var leftIndices = Enumerable.Repeat(leftRowIndex, rightRowCount).Select(i => (long)i).ToArray();
        var rightIndices = Enumerable.Range(0, rightRowCount).Select(i => (long)i).ToArray();

        return Filter != null
            ? ApplyJoinFilterToIndices(leftBatch, rightBatch, leftIndices, rightIndices, JoinSide.Left)
            : (leftIndices, rightIndices);
    }

    private (long[], long[]) ApplyJoinFilterToIndices(
        RecordBatch buildInputBuffer,
        RecordBatch probeBatch,
        long[] buildIndices,
        long[] probeIndices,
        JoinSide buildSide)
    {
        if (!buildIndices.Any() && !probeIndices.Any())
        {
            return (buildIndices, probeIndices);
        }

        var intermediateBatch = BuildBatchFromIndices(
            Filter!.Schema,
            buildInputBuffer,
            probeBatch,
            buildIndices.AsNullable(),
            probeIndices.AsNullable(),
            Filter.ColumnIndices,
            buildSide);

        var mask = ((BooleanColumnValue)Filter!.FilterExpression.Evaluate(intermediateBatch)).Values;

        var leftFiltered = buildIndices.Where((_, index) => mask[index]).ToArray();
        var rightFiltered = probeIndices.Where((_, index) => mask[index]).ToArray();

        return (leftFiltered, rightFiltered);
    }

    private RecordBatch BuildBatchFromIndices(
        Schema schema,
        RecordBatch buildInputBuffer,
        RecordBatch probeBatch,
        long?[] buildIndices,
        long?[] probeIndices,
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
                if (recordArray.Values.Count == 0 || buildIndices.NullCount() == buildIndices.Length)
                {
                    array = recordArray.NewEmpty(buildIndices.Length).Values;
                }
                else
                {
                    array = buildIndices.Select(i => i == null ? null : recordArray.Values[(int)i]).ToList();
                }
            }
            else
            {
                var recordArray = probeBatch.Results[columnIndex.Index];
                if (recordArray.Values.Count == 0 || probeIndices.NullCount() == probeIndices.Length)
                {
                    array = recordArray.NewEmpty(probeIndices.Length).Values;
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

    private (long?[] LeftIndices, long?[] RightIndices) AdjustIndicesByJoinType(
        long[] leftIndices,
        long[] rightIndices,
        int leftBatchRowCount,
        int rightBatchRowCount)
    {
        return JoinType switch
        {
            JoinType.Inner => (leftIndices.AsNullable(), rightIndices.AsNullable()),
            JoinType.Left => AppendLeftIndices(leftIndices, rightIndices, leftIndices.GetAntiIndices(leftBatchRowCount)),
            JoinType.LeftSemi => (leftIndices.GetSemiIndices(leftBatchRowCount).AsNullable(), rightIndices.AsNullable()),
            JoinType.LeftAnti => (leftIndices.GetAntiIndices(leftBatchRowCount).AsNullable(), rightIndices.AsNullable()),
            JoinType.Right or JoinType.Full => AppendRightIndices(leftIndices, rightIndices, rightIndices.GetAntiIndices(rightBatchRowCount)),
            JoinType.RightSemi => (leftIndices.AsNullable(), rightIndices.GetSemiIndices(rightBatchRowCount).AsNullable()),
            JoinType.RightAnti => (leftIndices.AsNullable(), rightIndices.GetAntiIndices(rightBatchRowCount).AsNullable()),
            
            _ => throw new NotImplementedException("AdjustIndicesByJoinType Implement join type")
        };
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