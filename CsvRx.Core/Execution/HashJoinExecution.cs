//using System.Runtime.InteropServices.ComTypes;
//using CsvRx.Core.Data;
//using CsvRx.Core.Physical.Joins;
//using SqlParser.Ast;

//namespace CsvRx.Core.Execution;

//internal record HashJoinExecution(
//    IExecutionPlan Left,
//    IExecutionPlan Right,
//    List<JoinOn> On,
//    JoinFilter? Filter,
//    JoinType JoinType,
//    PartitionMode PartitionMode,
//    List<ColumnIndex> ColumnIndices,
//    bool NullEqualsNull,
//    Schema Schema) : IExecutionPlan
//{
//    public IAsyncEnumerable<RecordBatch> Execute(QueryOptions options)
//    {
//        throw new NotImplementedException();
//    }
//}