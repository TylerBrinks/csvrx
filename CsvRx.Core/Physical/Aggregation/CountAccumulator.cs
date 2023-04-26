namespace CsvRx.Core.Physical.Aggregation;

internal record CountAccumulator : Accumulator
{
    public override void Accumulate(object value)
    {
        throw new NotImplementedException();
    }

    public override object Value => null;
}