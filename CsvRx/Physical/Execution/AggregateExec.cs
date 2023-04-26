using CsvRx.Data;
using CsvRx.Physical.Expressions;

namespace CsvRx.Physical.Execution;

public enum AggregationMode
{
    Partial,
    Final, 
    FinalPartitioned
}

public record AggregateExec(
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
        var fields = new List<Field>();

        foreach (var thing in groupBy)
        {
            fields.Add(new Field(thing.Name, thing.Expression.GetDataType(planSchema)));
        }

        if (mode == AggregationMode.Partial)
        {
            foreach (var expr in aggregateExpressions)
            {
                fields.AddRange(expr.StateFields);
            }
        }
        else
        {
            foreach (var expr in aggregateExpressions)
            {
                fields.Add(expr.Field);
            }
        }

        return new Schema(fields);
    }

    public List<IPhysicalExpression> OutputGroupExpr()
    {
        return GroupBy.Expr.Select((e, i) => (IPhysicalExpression) new PhysicalColumn(e.Name, i)).ToList();
    }

    public RecordBatch Execute()
    {
        return Plan.Execute();
    }
}