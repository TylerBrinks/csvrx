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
        RecordBatch aggregateBatch;

        if (!GroupBy.Expression.Any())
        {
            aggregateBatch = await new NoGroupingAggregation(Mode, Schema, AggregateExpressions, Plan, options).Aggregate();
        }
        else
        {
            aggregateBatch = await new GroupedHashAggregation(Mode, Schema, AggregateExpressions, Plan, GroupBy, options).Aggregate();
        }

        if (Mode == AggregationMode.Partial)
        {
            // Partial mode creates a single batch with aggregated values
            // therefore the single batch is returned 
            yield return aggregateBatch;
        }
        else
        {
            // Final mode receives a single batch which may be larger than
            // the configured batch size and is therefore repartitioned
            foreach (var batch in aggregateBatch.Repartition(options.BatchSize))
            {
                yield return batch;
            }
        }
    }
}