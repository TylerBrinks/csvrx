namespace CsvRx.Core.Physical.Aggregation;

public abstract record Accumulator
{
    public abstract void Accumulate(object value);

    public abstract object Value { get; }
}