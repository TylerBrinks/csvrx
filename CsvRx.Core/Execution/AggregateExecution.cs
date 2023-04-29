using CsvRx.Core.Data;
using CsvRx.Core.Physical;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Core.Physical.Expressions;
using System.Threading;

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
        var schema = CreateSchema(plan.Schema, groupBy.Expr, aggregateExpressions, mode);

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

    public List<IPhysicalExpression> OutputGroupExpr()
    {
        return GroupBy.Expr.Select((e, i) => (IPhysicalExpression)new Column(e.Name, i)).ToList();
    }

    public async IAsyncEnumerable<RecordBatch> Execute()
    {
        if (!GroupBy.Expr.Any())
        {
            await foreach (var batch in new NoGroupingAggregation(Mode, Schema, AggregateExpressions, Plan ))
            {
                yield return batch;
            }
        }
        else
        {
            await foreach (var batch in new GroupedHashAggregation(Schema,  GroupBy, AggregateExpressions, Plan))
            {
                yield return batch;
            }
        }
    }

    //private List<IPhysicalExpression> MergeExpressions(int columnIndex, Aggregate aggregate)
    //{
    //    return aggregate.StateFields
    //        .Select((f, i) => (IPhysicalExpression)new Column(f.Name, columnIndex + i))
    //        .ToList();
    //}
}