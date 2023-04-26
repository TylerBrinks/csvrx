namespace CsvRx.Core.Physical.Aggregation;

internal record SumAccumulator : Accumulator
{
    public override void Accumulate(object value)
    {
        throw new NotImplementedException();
    }
    public override object Value => null;
}