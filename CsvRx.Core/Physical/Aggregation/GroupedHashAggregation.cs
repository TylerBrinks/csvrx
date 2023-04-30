using CsvRx.Core.Data;
using CsvRx.Core.Execution;
using CsvRx.Core.Physical.Expressions;
using CsvRx.Core.Physical.Functions;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Aggregation;

internal class GroupedHashAggregation : IAsyncEnumerable<RecordBatch>
{
    private readonly IExecutionPlan _plan;
    private readonly GroupBy _groupBy;
    private readonly Schema _schema;
    private readonly List<Aggregate> _aggregates;
    private readonly AggregationMode _aggregationMode;

    public GroupedHashAggregation(
        AggregationMode aggregationMode,
        Schema schema,
        GroupBy groupBy,
        List<Aggregate> aggregates,
        IExecutionPlan plan
    )
    {
        _aggregationMode = aggregationMode;
        _plan = plan;
        _groupBy = groupBy;
        _schema = schema;
        _aggregates = aggregates;
    }

    public async IAsyncEnumerator<RecordBatch> GetAsyncEnumerator(CancellationToken cancellationToken = new ())
    {
        var map = new Dictionary<Sequence<object>, List<Accumulator>>();

        await foreach (var batch in _plan.Execute().WithCancellation(cancellationToken))
        {
            var groupKey = _groupBy.Expr.Select(e => e.Expression.Evaluate(batch)).ToList();

            var aggregateInputValues = GetAggregateInputs(batch);

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
                    accumulators = _aggregates.Cast<IAggregation>().Select(fn => fn.CreateAccumulator()).ToList();
                    map.Add(rowKey, accumulators);
                }

                for (var i = 0; i < accumulators.Count; i++)
                {
                    var value = aggregateInputValues[i].GetValue(rowIndex);
                    accumulators[i].Accumulate(value);
                }
            }
        }

        // Result batch containing the final aggregate values
        var aggregatedBatch = new RecordBatch(_schema);

        for (var i = 0; i < map.Count; i++)
        {
            var groupKey = map.Keys.Skip(i).First();
            var accumulators = map[groupKey];

            var groupCount = _groupBy.Expr.Count;

            for (var j = 0; j < groupCount; j++)
            {
                aggregatedBatch.Results[j].Add(groupKey[j]);
            }

            for (var j = 0; j < _aggregates.Count; j++)
            {
                aggregatedBatch.Results[groupCount + j].Add(accumulators[j].Value);
            }
        }

        yield return aggregatedBatch;
    }

    private List<ColumnValue> GetAggregateInputs(RecordBatch batch)
    {
        if (_aggregationMode == AggregationMode.Partial)
        {
            return _aggregates.Select(ae => ae.Expression.Evaluate(batch)).ToList();
        }

        var index = _groupBy.Expr.Count;
        return _aggregates.Select(ae => ae.Expression.Evaluate(batch, index++)).ToList();
    }
}