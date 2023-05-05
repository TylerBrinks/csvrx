using CsvRx.Core.Data;
using CsvRx.Core.Execution;
using CsvRx.Core.Physical.Expressions;
using CsvRx.Core.Physical.Functions;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Aggregation;

internal class NoGroupingAggregation : IAsyncEnumerable<RecordBatch>
{
    private readonly IExecutionPlan _plan;
    private readonly Schema _schema;
    private readonly List<Aggregate> _aggregateExpressions;
    private readonly AggregationMode _aggregationMode;
    private readonly QueryOptions _queryOptions;

    public NoGroupingAggregation(AggregationMode aggregationMode,
        Schema schema,
        List<Aggregate> aggregateExpressions,
        IExecutionPlan plan, 
        QueryOptions queryOptions)
    {
        _plan = plan;
        _schema = schema;
        _aggregateExpressions = aggregateExpressions;
        _aggregationMode = aggregationMode;
        _queryOptions = queryOptions;
    }

    public async IAsyncEnumerator<RecordBatch> GetAsyncEnumerator(CancellationToken cancellationToken = new ())
    {
        var accumulators = _aggregateExpressions.Cast<IAggregation>().Select(fn => fn.CreateAccumulator()).ToList();
        var expressions = MapAggregateExpressions(0);

        await foreach (var batch in _plan.Execute(_queryOptions).WithCancellation(cancellationToken))
        {
            AggregateBatch(batch, accumulators, expressions);
        }
            
        var columns = FinalizeAggregation(accumulators);
        yield return RecordBatch.TryNew(_schema, columns);
    }

    public void AggregateBatch(
        RecordBatch batch, 
        List<Accumulator> accumulators,
        List<List<IPhysicalExpression>> expressions)
    {
        foreach (var (accumulator, exp) in accumulators.Zip(expressions))
        {
            var values = exp.Select(c => (ArrayColumnValue)c.Evaluate(batch)).ToList();

            if (_aggregationMode == AggregationMode.Partial)
            {
                accumulator.UpdateBatch(values);
            }
            else
            {
                accumulator.MergeBatch(values);
            }
        }
    }

    public List<List<IPhysicalExpression>> MapAggregateExpressions(int columnIndex)
    {
        if (_aggregationMode == AggregationMode.Partial)
        {
            return _aggregateExpressions.Select(ae => ae.Expressions).ToList();
        }

        var index = columnIndex;
        return _aggregateExpressions.Select(agg =>
        {
            var expressions = MergeExpressions(index, agg);
            index += expressions.Count;
            return expressions;
        }).ToList();
    }

    private static List<IPhysicalExpression> MergeExpressions(int index, Aggregate aggregate)
    {
        return aggregate.StateFields.Select((f, i) => (IPhysicalExpression)new Column(f.Name, index + i)).ToList();
    }

    private List<object?> FinalizeAggregation(IEnumerable<Accumulator> accumulators)
    {
        if (_aggregationMode == AggregationMode.Partial)
        {
            return accumulators
                .Select(acc => acc.State)
                .Select(val => val.Select(sv => sv.RawValue))
                .SelectMany(_ => _)
                .ToList();
        }

        return accumulators
            .Select(acc => acc.Evaluate.RawValue)
            .ToList();
    }
}