namespace CsvRx.Core.Physical.Aggregation;

public record SumAccumulator : Accumulator
{
    public override void Accumulate(object value)
    {
        throw new NotImplementedException();
    }
    public override object Value => null;
}