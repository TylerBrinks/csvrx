using CsvRx.Core.Logical.Values;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Aggregation;

internal record MaxAccumulator : Accumulator
{
    private double? _value;

    public override void Accumulate(object? value)
    {
        if (value == null)
        {
            return;
        }

        if (_value == null)
        {
            _value = Convert.ToDouble(value);
        }
        else
        {
            _value = value switch
            {
                int i when i > _value => Convert.ToDouble(i),
                long l when l > _value => Convert.ToDouble(l),
                double d when d > _value => d,
                _ => _value
            };
        }
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
        UpdateBatch(values);
    }

    public override object? Value => _value;

    public override List<ScalarValue> State => new() { Evaluate };

    public override ScalarValue Evaluate => new IntegerScalar((long)_value);
}