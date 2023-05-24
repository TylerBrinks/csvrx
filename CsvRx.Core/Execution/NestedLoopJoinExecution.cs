using CsvRx.Core.Data;
using CsvRx.Core.Physical.Joins;

namespace CsvRx.Core.Execution;

internal record NestedLoopJoinExecution(
    IExecutionPlan Left,
    IExecutionPlan Right,
    JoinFilter? Filter,
    JoinType JoinType,
    List<ColumnIndex> ColumnIndices,
    Schema Schema) : JoinExecution, IExecutionPlan
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

    private RecordBatch JointLeftAndRightBatch(
        RecordBatch leftBatch, 
        RecordBatch rightBatch, 
        List<ColumnIndex> columnIndices, 
        bool[] visitedLeftSide)
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

        var (leftSide, rightSide) = AdjustIndicesByJoinType(leftIndices.ToArray(), rightIndices.ToArray(), leftBatch.RowCount, rightBatch.RowCount, JoinType);

        return BuildBatchFromIndices(Schema, leftBatch, rightBatch, leftSide, rightSide, columnIndices, JoinSide.Left);
    }

    private (long[], long[]) BuildJoinIndices(int leftRowIndex, RecordBatch rightBatch, RecordBatch leftBatch)
    {
        var rightRowCount = rightBatch.RowCount;
        var leftIndices = Enumerable.Repeat(leftRowIndex, rightRowCount).Select(i => (long)i).ToArray();
        var rightIndices = Enumerable.Range(0, rightRowCount).Select(i => (long)i).ToArray();

        return Filter != null
            ? ApplyJoinFilterToIndices(leftBatch, rightBatch, leftIndices, rightIndices, Filter, JoinSide.Left)
            : (leftIndices, rightIndices);
    }

    protected static (long?[] LeftIndices, long?[] RightIndices) AdjustIndicesByJoinType(
        long[] leftIndices,
        long[] rightIndices,
        int leftBatchRowCount,
        int rightBatchRowCount,
        JoinType joinType)
    {
        return joinType switch
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
}