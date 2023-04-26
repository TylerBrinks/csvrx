using System.Runtime.InteropServices;
using CsvRx.Core.Data;
using CsvRx.Core.Physical.Expressions;
using CsvRx.Data;
using CsvRx.Physical;

namespace CsvRx.Core.Physical.Execution;

public record FilterExec(IPhysicalExpression Predicate, IExecutionPlan Plan) : IExecutionPlan
{
    public Schema Schema => Plan.Schema;

    public static FilterExec TryNew(IPhysicalExpression predicate, IExecutionPlan plan)
    {
        var dt = predicate.GetDataType(plan.Schema);
        if (dt != ColumnDataType.Boolean)
        {
            throw new InvalidOleVariantTypeException("invalid filter expression");
        }

        return new FilterExec(predicate, plan);
    }

    public IEnumerable<RecordBatch> Execute()
    {
        var batches = Plan.Execute();

        foreach (var batch in batches)
        {
            var filterFlags = (BooleanColumnValue) Predicate.Evaluate(batch);

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
                    column.Array.RemoveAt(i);
                }
            }

            yield return batch;
        }
    }
}