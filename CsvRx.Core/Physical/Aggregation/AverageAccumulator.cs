using CsvRx.Core.Data;
using CsvRx.Core.Logical.Values;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Aggregation;

internal record AverageAccumulator(ColumnDataType DataType) : Accumulator
{
    private long _count;
    private double _sum;

    public override void Accumulate(object? value)
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

    public override object Value => CalculateAverage();

    public override List<ScalarValue> State => new() { Evaluate };

    public override ScalarValue Evaluate => CalculateAverageValue();

    private ScalarValue CalculateAverageValue()
    {
        var average = CalculateAverage();

        return DataType switch
        {
            ColumnDataType.Integer => new IntegerScalar(Convert.ToInt64(average)),
            _ => new DoubleScalar(average)
        };
    }

    private double CalculateAverage()
    {
        return _count == 0 ? 0 : _sum / _count;
    }
}