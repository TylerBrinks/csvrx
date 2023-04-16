using CsvRx.Data;

namespace CsvRx.Physical;

public record SelectionExec(IPhysicalPlan Plan, PhysicalExpression Expr) : IPhysicalPlan
{
    public Schema Schema => Plan.Schema;
    public List<RecordBatch> Execute()
    {
        var input = Plan.Execute();
        return input.Select(batch =>
        {
            //TODO
            ////var result = Expr.Evaluate(batch).
            //var schema = batch.Schema;
            //var columnCount = schema.Fields.Count;
            //var filteredFields = Enumerable.Range(0, columnCount).Select(_ => Filter(batch.Field(_))).ToList();

            return new RecordBatch(Plan.Schema, new List<ColumnVector>());
        }).ToList();
    }

    //private FieldVector Filter(ColumnVector v, BitVector selection)
    //{var filteredVector = VarCharVector("v")
    //}

    public List<IPhysicalPlan> Children => new() {Plan};
}