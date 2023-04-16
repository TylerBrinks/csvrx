using System.Data;
using CsvRx.Data;

namespace CsvRx.Physical;

public record ProjectionExec(IPhysicalPlan Plan, Schema Schema, List<PhysicalExpression> Expr):IPhysicalPlan
{
    public List<RecordBatch> Execute()
    {
        return Plan.Execute().Select(batch =>
        {
            var columns = Expr.Select(_ => _.Evaluate(batch)).ToList();
            return new RecordBatch(Schema, columns);
        }).ToList();
    }

    public List<IPhysicalPlan> Children => new() {Plan};
}

public record HashAggregateExec(
    IPhysicalPlan Input,
    List<PhysicalExpression> GroupExpr,
    List<IPhysicalAggregateExpression> AggregateExpr,
    Schema Schema) : IPhysicalPlan
{
    public List<RecordBatch> Execute()
    {
        var map = new Dictionary<List<object>, List<IAccumulator>>();

        // for each batch from the input executor
        foreach (var batch in Input.Execute())
        {
            // evaluate the grouping expressions
            var groupKeys = GroupExpr.Select(_ => _.Evaluate(batch)).ToList();

            // evaluate the expressions that are inputs to the aggregate functions
            var aggrInputValues = AggregateExpr.Select(_ => _.InputExpression.Evaluate(batch)).ToList();

            for (var i = 0; i < batch.RowCount; i++)
            {
                // create the key for the hash map
                var rowKey = groupKeys.Select(_ =>
                {
                    var value = _.GetValue(i);

                    //TODO
                    return value;
                }).ToList();

                var ac = AggregateExpr.Select(_ => _.CreateAccumulator()).ToList();
                map.TryAdd(rowKey, ac);
                var accumulators = map[rowKey];

                for (var j = 0; j < accumulators.Count; j++)
                {
                    var value = aggrInputValues[j].GetValue(i);
                    accumulators[j].Accumulate(value);
                }
            }
        }

        // create result batch containing final aggregate values
        //var root = new VectorSchemaRoot().Create(schema, new RootAllocator());
        //root.AllocateNew();
        //root.RowCount = map.Count;

        //var buildrs = root.FieldVectors.Select(_ => new ArrowVectorBuilder(_));

        //for (var i = 0; i < map.Count; i++)

        var index 
        foreach(var entry in map)
        {
            var index = map.Keys.
            var rowIndex = 
        }
    }

    public List<IPhysicalPlan> Children => new () { Input };
}