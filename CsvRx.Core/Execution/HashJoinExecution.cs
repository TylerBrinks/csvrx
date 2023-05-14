using CsvRx.Core.Data;
using CsvRx.Core.Physical.Joins;

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
    Schema Schema) : IExecutionPlan
{
    public IAsyncEnumerable<RecordBatch> Execute(QueryOptions options)
    {
        throw new NotImplementedException();
    }
}

internal record NestedLoopJoinExecution(
    IExecutionPlan Left,
    IExecutionPlan Right,
    JoinFilter? Filter, 
    JoinType JoinType,
    List<ColumnIndex> ColumnIndices,
    Schema Schema) : IExecutionPlan
{
    public IAsyncEnumerable<RecordBatch> Execute(QueryOptions options)
    {
        throw new NotImplementedException();
    }
}