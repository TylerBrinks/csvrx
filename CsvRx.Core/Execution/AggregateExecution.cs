using CsvRx.Core.Data;
using CsvRx.Core.Physical;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Core.Physical.Expressions;

namespace CsvRx.Core.Execution;

public enum AggregationMode
{
    Partial,
    Final
}

internal record AggregateExecution(
    AggregationMode Mode,
    GroupBy GroupBy,
    List<Aggregate> AggregateExpressions,
    IExecutionPlan Plan,
    Schema Schema,
    Schema InputSchema
    ) : IExecutionPlan
{
    public static AggregateExecution TryNew(
        AggregationMode mode,
        GroupBy groupBy,
        List<Aggregate> aggregateExpressions,
        IExecutionPlan plan,
        Schema inputSchema)
    {
        var schema = CreateSchema(plan.Schema, groupBy.Expression, aggregateExpressions, mode);

        return new AggregateExecution(mode, groupBy, aggregateExpressions, plan, schema, inputSchema);
    }

    private static Schema CreateSchema(
        Schema planSchema,
        IEnumerable<(IPhysicalExpression Expression, string Name)> groupBy,
        List<Aggregate> aggregateExpressions,
        AggregationMode mode)
    {
        var fields = groupBy
            .Select(group => new Field(group.Name, group.Expression.GetDataType(planSchema)))
            .ToList();

        if (mode == AggregationMode.Partial)
        {
            foreach (var expr in aggregateExpressions)
            {
                fields.AddRange(expr.StateFields);
            }
        }
        else
        {
            fields.AddRange(aggregateExpressions.Select(expr => expr.Field));
        }

        return new Schema(fields);
    }

    public List<IPhysicalExpression> OutputGroupExpression()
    {
        return GroupBy.Expression.Select((e, i) => (IPhysicalExpression)new Column(e.Name, i)).ToList();
    }

    public async IAsyncEnumerable<RecordBatch> Execute(QueryOptions options)
    {
        if (!GroupBy.Expression.Any())
        {
            await foreach (var batch in new NoGroupingAggregation(Mode, Schema, AggregateExpressions, Plan, options))
            {
                yield return batch;
            }
        }
        else
        {
            await foreach (var batch in new GroupedHashAggregation(Mode, Schema,  GroupBy, AggregateExpressions, Plan, options))
            {
                yield return batch;
            }
        }
    }
}