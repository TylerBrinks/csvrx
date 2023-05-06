using CsvRx.Core.Logical.Values;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Aggregation;

internal record AverageAccumulator : Accumulator
{
    private long _count;
    private double _sum;

    public override void Accumulate(object value)
    {
        if (value == null)
        {
            return;
        }

        _count++;
        _sum += Convert.ToDouble(value);
    }

    public override void UpdateBatch(List<ArrayColumnValue> values)
    {
        var array = values[0];

        _count += array.Values.Count;

        foreach (var item in array.Values)
        {
            _sum += Convert.ToDouble(item);
        }
    }

    public override void MergeBatch(List<ArrayColumnValue> values)
    {
        UpdateBatch(values);
    }

    public override object? Value
    {
        get
        {
            if (_sum == 0)
            {
                return 0;
            }

            return _sum / _count;
        }
    }

    public override ScalarValue Evaluate => _count == 0 ? new DoubleScalar(0) : new DoubleScalar(_sum / _count); 

    public override List<ScalarValue> State => new() { Evaluate };
}