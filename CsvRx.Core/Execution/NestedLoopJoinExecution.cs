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
            await foreach (var leftBatch in Left.Execute(options))
            {
                await foreach (var rightBatch in Right.Execute(options))
                {
                    throw new NotImplementedException("Implement left build side");
                }
            }
        }
        else
        {
            await foreach (var rightBatch in Right.Execute(options))
            {
                await foreach (var leftBatch in Left.Execute(options))
                {
                    yield return JointLeftAndRightBatch(leftBatch, rightBatch, ColumnIndices);
                }
            }
        }
    }

    private RecordBatch JointLeftAndRightBatch(RecordBatch leftBatch, RecordBatch rightBatch, List<ColumnIndex> columnIndices)
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
            //todo: implement full join
            /*left_side.iter().flatten().for_each(|x| {
                    visited_left_side.set_bit(x as usize, true);
                });*/
        }

        var (leftSide, rightSide) = AdjustIndicesByJoinType(leftIndices.ToArray(), rightIndices.ToArray(), leftBatch.RowCount, rightBatch.RowCount);

        return BuildBatchFromIndices(Schema, leftBatch, rightBatch, leftSide, rightSide, columnIndices, JoinSide.Left);
    }

    private (long[], long[]) BuildJoinIndices(int leftRowIndex, RecordBatch rightBatch, RecordBatch leftBatch)
    {
        var rightRowCount = rightBatch.RowCount;
        var leftIndices = Enumerable.Repeat(leftRowIndex, rightRowCount).Select(i=>(long)i).ToArray();
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
                if (recordArray.Values.Count == 0)// || buildIndices.nullCount == buildIndices.Length)
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
                if (recordArray.Values.Count == 0) //  probe_indices.null_count() == probe_indices.len()
                {
                    array = recordArray.NewEmpty(probeIndices.Length).Values;
                }
                else
                {
                    array = probeIndices.Select(i => i == null ? null : recordArray.Values[(int) i]).ToList();
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
        switch (JoinType)
        {
            case JoinType.Inner:
                return (leftIndices.AsNullable(), rightIndices.AsNullable());

            case JoinType.Left:
            {
                var leftUnmatchedIndices = GetAntiLongIndices(leftBatchRowCount, leftIndices);
                return AppendLeftIndices(leftIndices, rightIndices, leftUnmatchedIndices);
            }
            case JoinType.LeftSemi:
            {
                var leftSemiIndices = GetSemiLongIndices(leftBatchRowCount, leftIndices);
                return (leftSemiIndices.AsNullable(), rightIndices.AsNullable());
            }
            case JoinType.LeftAnti:
            {
                var leftAntiIndices = GetAntiLongIndices(leftBatchRowCount, leftIndices);
                return (leftAntiIndices.AsNullable(), rightIndices.AsNullable());
            }
            case JoinType.Right or JoinType.Full:
            {
                var rightUnmatchedIndices = GetAntiIndices(rightBatchRowCount, rightIndices);
                return AppendRightIndices(leftIndices, rightIndices, rightUnmatchedIndices);
            }
            case JoinType.RightSemi:
            {
                var rightSemiIndices = GetSemiLongIndices(rightBatchRowCount, rightIndices);
                return (leftIndices.AsNullable(), rightSemiIndices.AsNullable());
            }
            case JoinType.RightAnti:
            {
                var rightAntiIndices = GetAntiIndices(rightBatchRowCount, rightIndices);
                return (leftIndices.AsNullable(), rightAntiIndices.AsNullable());
            }
            default:
                throw new NotImplementedException("AdjustIndicesByJoinType Implement join type");
        }
    }

    private static (long?[] LeftIndices, long?[] RightIndices) AppendLeftIndices(long[] leftIndices, long[] rightIndices, long[] leftUnmatchedIndices)
    {
        var unmatchedSize = leftUnmatchedIndices.Length;
        if (unmatchedSize == 0)
        {
            return (leftIndices.AsNullable(), rightIndices.AsNullable());
        }

        var newLeftIndices = leftIndices.Concat(leftUnmatchedIndices).AsNullable();
        var newRightIndices = rightIndices.AsNullable().Concat(new long?[unmatchedSize]).ToArray();

        return (newLeftIndices, newRightIndices);
    }

    private static (long?[] LeftIndices, long?[] RightIndices) AppendRightIndices(long[] leftIndices, long[] rightIndices, long[] rightUnmatchedIndices)
    {
        throw new NotImplementedException();
    }

    private static long[] GetAntiIndices(int rightBatchRowCount, long[] rightIndices)
    {
        throw new NotImplementedException();
    }
    /// <summary>
    /// Gets unmatched de-duplicated indices
    /// </summary>
    /// <param name="rowCount">Number of rows in the current batch</param>
    /// <param name="inputIndices">Left side index values</param>
    /// <returns>Anti-index values (inverse of matched/true values)</returns>
    private static long[] GetAntiLongIndices(int rowCount, long[] inputIndices)
    {
        var bitmap = new bool[rowCount];

        foreach (var inputIndex in inputIndices)
        {
            bitmap[inputIndex] = true;
        }

        var antiIndices = Enumerable.Range(0, rowCount)
            .Select(index => ((long)index, !bitmap[index]))
            .Where(i => i.Item2)
            .Select(i => i.Item1);

        return antiIndices.ToArray();
    }

    private static long[] GetSemiLongIndices(int leftBatchRowCount, long[] leftIndices)
    {
        throw new NotImplementedException();
    }
}