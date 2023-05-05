using CsvRx.Core.Data;
using CsvRx.Core.Physical.Expressions;
using CsvRx.Core.Values;

namespace CsvRx.Core.Execution;

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

    public async IAsyncEnumerable<RecordBatch> Execute(QueryOptions options)
    {
        await foreach (var batch in Plan.Execute(options))
        {
            var columns = Expressions.Select(e => e.Expression.Evaluate(batch)).ToList();

            var projection = new RecordBatch(Schema);

            for (var i = 0; i < columns.Count; i++)
            {
                var column = columns[i];

                var array = (ArrayColumnValue)column;
                foreach (var val in array.Values)
                {
                    projection.Results[i].Add(val);
                }
            }

            yield return projection;
        }
    }
}