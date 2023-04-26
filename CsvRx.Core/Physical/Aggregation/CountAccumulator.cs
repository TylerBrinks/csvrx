namespace CsvRx.Core.Physical.Aggregation;

public record CountAccumulator : Accumulator
{
    public override void Accumulate(object value)
    {
        throw new NotImplementedException();
    }

    public override object Value => null;
}