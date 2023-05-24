using CsvRx.Core.Data;
using CsvRx.Core.Physical.Expressions;
using CsvRx.Core.Physical.Joins;
using CsvRx.Core.Values;

namespace CsvRx.Core.Execution;

internal record HashJoinExecution(
    IExecutionPlan Left,
    IExecutionPlan Right,
    List<JoinOn> On,
    JoinFilter? Filter,
    JoinType JoinType,
    PartitionMode PartitionMode,
    List<ColumnIndex> ColumnIndices,
    bool NullEqualsNull,
    Schema Schema) : JoinExecution, IExecutionPlan
{
    public async IAsyncEnumerable<RecordBatch> Execute(QueryOptions options)
    {
        var onLeft = On.Select(j => j.Right).ToList();
        var onRight = On.Select(j => j.Left).ToList();

        if (PartitionMode == PartitionMode.CollectLeft)
        {
            var leftData = await CollectLeft(options, onLeft);

            // these join type need the bitmap to identify which row has be matched or unmatched.
            // For the `left semi` join, need to use the bitmap to produce the matched row in the left side
            // For the `left` join, need to use the bitmap to produce the unmatched row in the left side with null
            // For the `left anti` join, need to use the bitmap to produce the unmatched row in the left side
            // For the `full` join, need to use the bitmap to produce the unmatched row in the left side with null
            var visitedLeftSide = NeedProduceResultInFinal(JoinType)
                ? new bool[leftData.Batch.RowCount]
                : Array.Empty<bool>();

            await foreach (var rightBatch in Right.Execute(options))
            {
                if (rightBatch.RowCount > 0)
                {
                    var indices = BuildJoinIndices(rightBatch, leftData, onLeft, onRight, Filter, 0, JoinSide.Left);

                    //if (indices.LeftIndies.Any() && indices.RightIndices.Any())
                    {
                        // Set the left bitmap
                        // Only left, full, left semi, left anti need the left bitmap
                        if (NeedProduceResultInFinal(JoinType))
                        {
                            foreach (var index in indices.LeftIndies)
                            {
                                visitedLeftSide[(int)index] = true;
                            }
                        }

                        var (leftSide, rightSide) = AdjustIndicesByJoinType(indices.LeftIndies, indices.RightIndices,
                            rightBatch.RowCount, JoinType);

                        var batch = BuildBatchFromIndices(Schema, leftData.Batch, rightBatch,
                            leftSide, rightSide, ColumnIndices, JoinSide.Left);

                        yield return batch;
                    }
                    //else
                    //{
                    //    throw new InvalidOperationException("Failed to build join indices");
                    //}
                }
                else
                {
                    if (NeedProduceResultInFinal(JoinType)) //TODO: && isExhausted
                    {

                        var (leftSide, rightSide) = GetFinalIndices(visitedLeftSide, JoinType);
                        var rightEmptyBatch = new RecordBatch(Right.Schema);

                        yield return BuildBatchFromIndices(Schema, leftData.Batch, rightEmptyBatch,
                            leftSide, rightSide, ColumnIndices, JoinSide.Left);
                    }
                }
            }
        }
        else if (PartitionMode == PartitionMode.Partitioned)
        {

        }
    }
    private static bool NeedProduceResultInFinal(JoinType joinType)
    {
        return joinType is JoinType.Left or JoinType.LeftAnti or JoinType.LeftSemi or JoinType.Full;
    }

    private static (long?[] LeftIndices, long?[] RightIndices) AdjustIndicesByJoinType(
        long[] leftIndices,
        long[] rightIndices,
        int countRightBatch,
        JoinType joinType)
    {
        return joinType switch
        {
            JoinType.Inner or JoinType.Left => (leftIndices.AsNullable(), rightIndices.AsNullable()),

            // Unmatched right row will be produced in this batch
            // Combine the matched and unmatched right result together
            JoinType.Right or JoinType.Full => AppendRightIndices(leftIndices, rightIndices, rightIndices.GetAntiIndices(countRightBatch)),

            // Remove duplicated records in the right side
            JoinType.RightSemi => (leftIndices.AsNullable(), rightIndices.GetSemiIndices(countRightBatch).AsNullable()),

            // Remove duplicated records in the right side
            JoinType.RightAnti => (leftIndices.AsNullable(), rightIndices.GetAntiIndices(countRightBatch).AsNullable()),

            // matched or unmatched left row will be produced in the end of loop
            // When visit the right batch, we can output the matched left row and don't need to wait the end of loop
            JoinType.LeftSemi or JoinType.LeftAnti => (Array.Empty<long?>(), Array.Empty<long?>())
        };

        /*
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
         */
    }

    private async Task<JoinLeftData> CollectLeft(QueryOptions options, IReadOnlyCollection<Column> onLeft)
    {
        // Left side builds require a single batch
        var joinBatch = new RecordBatch(Left.Schema);

        var offset = 0;
        var joinMap = new JoinMap();

        await foreach (var batch in Left.Execute(options))
        {
            var rowCount = batch.RowCount;

            var hashBuffer = new int[rowCount];
            UpdateHash(onLeft, batch, joinMap, offset, hashBuffer);

            offset += batch.RowCount;

            joinBatch.Concat(batch);
        }

        return new JoinLeftData(joinMap, joinBatch);
    }

    private static void UpdateHash(IEnumerable<Column> on, RecordBatch batch, JoinMap hashMap, int offset, int[] hashValues)
    {
        var keyValues = on.Select(c => c.Evaluate(batch)).ToList();

        CreateHashes(keyValues, hashValues);

        for (var row = 0; row < hashValues.Length; row++)
        {
            var hashValue = hashValues[row];
            var found = hashMap.TryGetValue(hashValue, out var list);

            if (found)
            {
                list!.Add(row + offset);
            }
            else
            {
                hashMap.Add(hashValue, new List<int> { row + offset });
            }
        }
    }

    private static void CreateHashes(List<ColumnValue> columnValues, int[] hashBuffer)
    {
        //TODO: multi column?
        //var multiColumn = columnValues.Count > 1;

        foreach (var columnValue in columnValues)
        {
            HashArray(columnValue, hashBuffer);
        }
    }

    private static void HashArray(ColumnValue columnValue, IList<int> hashBuffer)
    {
        for (var i = 0; i < columnValue.Size; i++)
        {
            var hash = columnValue.GetValue(i).GetHashCode(); //TODO handle nulls
            hashBuffer[i] = hash;
        }
    }

    private (long[] LeftIndies, long[] RightIndices) BuildJoinIndices(
        RecordBatch probeBatch,
        JoinLeftData joinData,
        IEnumerable<Column> onBuild,
        IEnumerable<Column> onProbe,
        JoinFilter? filter,
        int offset,
        JoinSide joinSide
    )
    {
        var buildInputBuffer = joinData.Batch;

        var (buildIndices, probeIndices) = BuildEqualConditionJoinIndices(joinData.JoinMap,
            buildInputBuffer, probeBatch, onBuild, onProbe, offset);

        if (filter != null)
        {
            return ApplyJoinFilterToIndices(buildInputBuffer, probeBatch, buildIndices, probeIndices, filter, joinSide);
        }

        return (buildIndices, probeIndices);
    }

    /// <summary>
    /// Returns build/probe indices satisfying the equality condition.
    /// On LEFT.b1 = RIGHT.b2
    /// LEFT Table:
    ///  a1  b1  c1
    ///  1   1   10
    ///  3   3   30
    ///  5   5   50
    ///  7   7   70
    ///  9   8   90
    ///  11  8   110
    /// 13   10  130
    /// RIGHT Table:
    ///  a2   b2  c2
    ///  2    2   20
    ///  4    4   40
    ///  6    6   60
    ///  8    8   80
    /// 10   10  100
    /// 12   10  120
    /// The result is
    /// +----+----+-----+----+----+-----+
    /// | a1 | b1 | c1  | a2 | b2 | c2  |
    /// +----+----+-----+----+----+-----+
    /// | 11 | 8  | 110 | 8  | 8  | 80  |
    /// | 13 | 10 | 130 | 10 | 10 | 100 |
    /// | 13 | 10 | 130 | 12 | 10 | 120 |
    /// | 9  | 8  | 90  | 8  | 8  | 80  |
    /// +----+----+-----+----+----+-----+
    /// And the result of build and probe indices are:
    /// Build indices:  5, 6, 6, 4
    /// Probe indices: 3, 4, 5, 3
    /// </summary>
    /// <param name="buildHashmap"></param>
    /// <param name="buildInputBuffer"></param>
    /// <param name="probeBatch"></param>
    /// <param name="onBuild"></param>
    /// <param name="onProbe"></param>
    /// <param name="offset"></param>
    /// <returns></returns>
    private static (long[] BuildIndices, long[] ProbeIndices) BuildEqualConditionJoinIndices(
        JoinMap buildHashmap,
        RecordBatch buildInputBuffer,
        RecordBatch probeBatch,
        IEnumerable<Column> onBuild,
        IEnumerable<Column> onProbe,
        int offset)
    {
        var keysValues = onProbe.Select(c => c.Evaluate(probeBatch)).ToList();
        var buildJoinValues = onBuild.Select(c => c.Evaluate(buildInputBuffer)).ToList();

        var hashBuffer = new int[probeBatch.RowCount];
        CreateHashes(keysValues, hashBuffer);

        var buildIndices = new List<long>();
        var probeIndices = new List<long>();

        for (var row = 0; row < hashBuffer.Length; row++)
        {
            var hashValue = hashBuffer[row];
            // Get the hash and find it in the build index

            // For every item on the build and probe we check if it matches
            // This possibly contains rows with hash collisions,
            // So we have to check here whether rows are equal or not

            var exists = buildHashmap.TryGetValue(hashValue, out var indices);
            if (exists)
            {
                foreach (var i in indices!)
                {

                    var offsetBuildIndex = i - offset;

                    if (!EqualRows(offsetBuildIndex, row, buildJoinValues, keysValues))
                    {
                        continue;
                    }

                    buildIndices.Add(offsetBuildIndex);
                    probeIndices.Add(row);
                }
            }
        }

        return (buildIndices.ToArray(), probeIndices.ToArray());
    }

    /// <summary>
    /// Left and right row have equal values
    /// If more data types are supported here, please also add the data types in can_hash function
    /// to generate hash join logical plan.
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <param name="leftArrays"></param>
    /// <param name="rightArrays"></param>
    /// <returns></returns>
    private static bool EqualRows(int left, int right, IEnumerable<ColumnValue> leftArrays, IEnumerable<ColumnValue> rightArrays)
    {

        return leftArrays.Zip(rightArrays).All(c =>
            {
                //todo: nulls equal
                //if left == null && right == null && null_equals_null

                var leftValue = c.First.GetValue(left);
                var rightValue = c.Second.GetValue(right);

                return leftValue.Equals(rightValue);
            }
        );
    }
}
