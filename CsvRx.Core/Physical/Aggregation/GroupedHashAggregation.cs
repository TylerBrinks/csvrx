using CsvRx.Core.Data;
using CsvRx.Core.Execution;
using CsvRx.Core.Physical.Expressions;
using CsvRx.Core.Physical.Functions;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Aggregation;

internal class GroupedHashAggregation
{
    private readonly IExecutionPlan _plan;
    private readonly GroupBy _groupBy;
    private readonly Schema _schema;
    private readonly List<Aggregate> _aggregates;
    private readonly AggregationMode _aggregationMode;
    private readonly QueryOptions _queryOptions;

    public GroupedHashAggregation(
        AggregationMode aggregationMode,
        Schema schema,
        List<Aggregate> aggregates,
        IExecutionPlan plan,
        GroupBy groupBy, 
        QueryOptions queryOptions)
    {
        _aggregationMode = aggregationMode;
        _plan = plan;
        _queryOptions = queryOptions;
        _groupBy = groupBy;
        _schema = schema;
        _aggregates = aggregates;
    }

    public async Task<RecordBatch> Aggregate(CancellationToken cancellationToken = new ())
    {
        if (_aggregationMode == AggregationMode.Partial)
        {
            return await Accumulate(cancellationToken);
        }

        // Final aggregation operates on a single batch
        var final = _plan.Execute(_queryOptions)
            .ToBlockingEnumerable()
            .First();

        return final;
    }

    private async Task<RecordBatch> Accumulate(CancellationToken cancellationToken)
    {
        var map = new Dictionary<Sequence<object>, List<Accumulator>>();

        await foreach (var batch in _plan.Execute(_queryOptions).WithCancellation(cancellationToken))
        {
            var groupKey = _groupBy.Expression.Select(e => e.Expression.Evaluate(batch)).ToList();

            var aggregateInputValues = GetAggregateInputs(batch);

            for (var rowIndex = 0; rowIndex < batch.RowCount; rowIndex++)
            {
                var keyList = groupKey.Select(key => key.GetValue(rowIndex)).ToList();

                var rowKey = new Sequence<object>(keyList!);

                map.TryGetValue(rowKey, out var accumulators);

                if (accumulators == null || accumulators.Count == 0)
                {
                    accumulators = _aggregates.Cast<IAggregation>().Select(fn => fn.CreateAccumulator()).ToList();
                    // select distinct... creates a grouping without an aggregation
                    // so the addition of the accumulator needs to handle possible
                    // duplicate row key values.
                    map.TryAdd(rowKey, accumulators);
                }

                for (var i = 0; i < accumulators.Count; i++)
                {
                    var value = aggregateInputValues[i].GetValue(rowIndex);
                    accumulators[i].Accumulate(value!);
                }
            }
        }

        // Result batch containing the final aggregate values
        var aggregatedBatch = new RecordBatch(_schema);

        for (var i = 0; i < map.Count; i++)
        {
            var groupKey = map.Keys.Skip(i).First();
            var accumulators = map[groupKey];

            var groupCount = _groupBy.Expression.Count;

            for (var j = 0; j < groupCount; j++)
            {
                aggregatedBatch.Results[j].Add(groupKey[j]);
            }

            for (var j = 0; j < _aggregates.Count; j++)
            {
                aggregatedBatch.Results[groupCount + j].Add(accumulators[j].Value);
            }
        }

        return aggregatedBatch;
    }

    private List<ColumnValue> GetAggregateInputs(RecordBatch batch)
    {
        if (_aggregationMode == AggregationMode.Partial)
        {
            return _aggregates.Select(ae => ae.Expression.Evaluate(batch)).ToList();
        }

        var index = _groupBy.Expression.Count;

        return _aggregates.Select(ae => ae.Expression.Evaluate(batch, index++)).ToList();
    }
}