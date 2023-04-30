using CsvRx.Core.Logical.Values;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Aggregation;

internal record CountAccumulator : Accumulator
{
    private uint _count;

    public override void Accumulate(object value)
    {
        _count++;
    }

    public override void UpdateBatch(List<ArrayColumnValue> values)
    {
        foreach (var value in values[0].Values)
        {
            Accumulate(value);
        }
    }

    public override void MergeBatch(List<ArrayColumnValue> values)
    {
        _count = Convert.ToUInt32(values[0].Values[0]);
    }

    public override object? Value => _count;

    public override List<ScalarValue> State => new() { Evaluate };

    public override ScalarValue Evaluate => new IntegerScalar(_count);
}