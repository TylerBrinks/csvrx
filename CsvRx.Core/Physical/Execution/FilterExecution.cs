using System.Runtime.InteropServices;
using CsvRx.Core.Data;
using CsvRx.Core.Physical.Expressions;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Execution;

internal record FilterExecution(IPhysicalExpression Predicate, IExecutionPlan Plan) : IExecutionPlan
{
    public Schema Schema => Plan.Schema;

    public static FilterExecution TryNew(IPhysicalExpression predicate, IExecutionPlan plan)
    {
        var dt = predicate.GetDataType(plan.Schema);
        if (dt != ColumnDataType.Boolean)
        {
            throw new InvalidOleVariantTypeException("invalid filter expression");
        }

        return new FilterExecution(predicate, plan);
    }

    public async IAsyncEnumerable<RecordBatch> Execute()
    {
        await foreach (var batch in Plan.Execute())
        {
            var filterFlags = (BooleanColumnValue)Predicate.Evaluate(batch);

            var filterIndices = new List<int>();
            for (var i = filterFlags.Size - 1; i >= 0; i--)
            {
                if (filterFlags.Values[i])
                {
                    continue;
                }

                filterIndices.Add(i);
            }

            foreach (var column in batch.Results)
            {
                foreach (var i in filterIndices.Where(i => !filterFlags.Values[i]))
                {
                    column.Values.RemoveAt(i);
                }
            }

            yield return batch;
        }
    }
}