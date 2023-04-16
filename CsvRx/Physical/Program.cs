
namespace CsvRx.Physical;

public abstract record PhysicalExpression
{
    public abstract ColumnVector Evaluate(RecordBatch input);
}

public record LiteralStringExpression(string Value) : PhysicalExpression
{
    public override ColumnVector Evaluate(RecordBatch input)
    {
        return new LiteralValueVector(Value.ToCharArray(), input.RowCount);
    }
}
