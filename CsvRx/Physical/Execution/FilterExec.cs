using System.Runtime.InteropServices;
using CsvRx.Data;
using CsvRx.Physical.Expressions;

namespace CsvRx.Physical.Execution;

public record FilterExec(IPhysicalExpression Predicate, IExecutionPlan ExecutionPlan) : IExecutionPlan
{
    public Schema Schema => ExecutionPlan.Schema;

    public static FilterExec TryNew(IPhysicalExpression predicate, IExecutionPlan plan)
    {
        var dt = predicate.GetDataType(plan.Schema);
        if (dt != ColumnDataType.Boolean)
        {
            throw new InvalidOleVariantTypeException("invalid filter expression");
        }

        return new FilterExec(predicate, plan);
    }
}