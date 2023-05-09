using CsvRx.Core.Data;
using CsvRx.Core.Logical.Values;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Aggregation;

internal record VarianceAccumulator(ColumnDataType DataType, StatisticType StatisticType) : Accumulator
{
    private long _count;
    private double _mean;
    private double _m2;

    public override void Accumulate(object? value)
    {
        if (value == null)
        {
            return;
        }

        var val = Convert.ToDouble(value);

        var newCount = _count + 1;
        var delta1 = val - _mean;
        var newMean = delta1 / newCount + _mean;
        var delta2 = val - newMean;
        var newM2 = _m2 + delta1 * delta2;
        _count += 1;
        _mean = newMean;
        _m2 = newM2;
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
        var counts = values[0];
        var means = values[1];
        var m2s = values[2];

        for (var i = 0; i < counts.Values.Count; i++)
        {
            var c = (long)counts.GetValue(i)!;

            if (c == 0)
            {
                continue;
            }

            var newCount = _count + c;
            var indexMean = (double)means.GetValue(i)!;

            var newMean = _mean * _count / newCount + indexMean * c / newCount;
            var delta = _mean - indexMean;
            var newM2 = _m2 + (double)m2s.GetValue(i)! + delta * delta * _count * c / newCount;

            _count = newCount;
            _mean = newMean;
            _m2 = newM2;
        }
    }

    public override object? Value => CalculateVariance();

    public override List<ScalarValue> State => new()
    {
        new IntegerScalar(_count),
        new DoubleScalar(_mean),
        new DoubleScalar(_m2)
    };

    public override ScalarValue Evaluate =>
        DataType == ColumnDataType.Integer
            ? new IntegerScalar(Convert.ToInt64(CalculateVariance()))
            : new DoubleScalar(CalculateVariance());

    public double CalculateVariance()
    {
        var count = GetCount();

        if (count is 0 or 1)
        {
            return 0;
        }

        return _m2 / count;
    }

    private long GetCount()
    {
        if (StatisticType == StatisticType.Population)
        {
            return _count;
        }

        if (_count > 0)
        {
            return _count - 1;
        }

        return _count;
    }
}