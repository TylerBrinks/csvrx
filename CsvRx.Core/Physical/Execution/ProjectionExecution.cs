using CsvRx.Core.Data;
using CsvRx.Core.Physical.Expressions;
using CsvRx.Physical;

namespace CsvRx.Core.Physical.Execution;

internal record ProjectionExecution(
    List<(IPhysicalExpression Expression, string Name)> Expressions, 
    Schema Schema,
    IExecutionPlan Plan) : IExecutionPlan
{
    public static IExecutionPlan TryNew(List<(IPhysicalExpression Expression, string Name)> physicalExpressions, IExecutionPlan plan)
    {
        var fields = physicalExpressions.Select(_ => new Field(_.Name, _.Expression.GetDataType(plan.Schema))).ToList();
        var schema = new Schema(fields);

        //TODO alias loop
        //TODO output ordering respect alias

        return new ProjectionExecution(physicalExpressions, schema, plan);//todo alias_map, metrics?
    }

    public IEnumerable<RecordBatch> Execute()
    {
        var batches = Plan.Execute();

        return batches;
    }
}