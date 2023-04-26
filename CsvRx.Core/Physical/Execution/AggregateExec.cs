using CsvRx.Core.Data;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Core.Physical.Expressions;
using CsvRx.Data;
using CsvRx.Physical;

namespace CsvRx.Core.Physical.Execution;

public enum AggregationMode
{
    Partial,
    Final
}

internal record AggregateExec(
    AggregationMode Mode,
    PhysicalGroupBy GroupBy,
    List<AggregateExpression> AggregateExpressions,
    IExecutionPlan Plan,
    Schema Schema,
    Schema InputSchema
    ) : IExecutionPlan
{
    public static AggregateExec TryNew(
        AggregationMode mode, 
        PhysicalGroupBy groupBy, 
        List<AggregateExpression> aggregateExpressions, 
        IExecutionPlan plan, 
        Schema inputSchema)
    {
        var schema = CreateSchema(plan.Schema, groupBy.Expr, aggregateExpressions, mode);

        return new AggregateExec(mode, groupBy, aggregateExpressions, plan, schema, inputSchema);
    }

    private static Schema CreateSchema(
        Schema planSchema,
        List<(IPhysicalExpression Expression, string Name)> groupBy, 
        List<AggregateExpression> aggregateExpressions, 
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
        return GroupBy.Expr.Select((e, i) => (IPhysicalExpression) new PhysicalColumn(e.Name, i)).ToList();
    }

    public IEnumerable<RecordBatch> Execute()
    {
        var map = new Dictionary<Sequence<object>, List<Accumulator>>();

        foreach (var batch in Plan.Execute())
        {
            var groupKey = GroupBy.Expr.Select(e => e.Expression.Evaluate(batch)).ToList();

            var aggregateInputValues = AggregateExpressions.Select(ae => ae.InputExpression.Evaluate(batch)).ToList();
            
            for (var rowIndex = 0; rowIndex < batch.RowCount; rowIndex++)
            {
                var keyList = groupKey.Select(key =>
                {
                    var value = key.GetValue(rowIndex);
                    return value;
                }).ToList();
                var rowKey = new Sequence<object>(keyList);

                map.TryGetValue(rowKey, out var accumulators);

                if (accumulators == null || accumulators.Count == 0)
                {
                    accumulators = AggregateExpressions.Select(ae => ae.CreateAccumulator()).ToList();
                    map.Add(rowKey, accumulators);
                }

                for (var i = 0; i < accumulators.Count; i++)
                {
                    var value = aggregateInputValues[i].GetValue(rowIndex);
                    accumulators[i].Accumulate(value);
                }
            }

            //yield return batch;
        }

        // Result batch containing the final aggregate values
        var aggregatedBatch = new RecordBatch(Schema);

        for (var i = 0; i < map.Count; i++)
        {
            var groupKey = map.Keys.Skip(i).First();
            var accumulators = map[groupKey];

            var groupCount = GroupBy.Expr.Count;

            for (var j = 0; j < groupCount; j++)
            {
                //var x = (i, groupKey[j]);
                aggregatedBatch.Results[j].Add(groupKey[j]);
            }

            for (var j = 0; j < AggregateExpressions.Count; j++)
            {
                //var x = (i, accumulators[j].Value);
                aggregatedBatch.Results[groupCount+j].Add(accumulators[j].Value);
            }
        }


        yield return aggregatedBatch;
    }
}