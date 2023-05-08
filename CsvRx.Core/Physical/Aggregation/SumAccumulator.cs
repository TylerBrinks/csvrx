using CsvRx.Core.Data;
using CsvRx.Core.Logical.Values;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Aggregation;

internal record SumAccumulator(ColumnDataType DataType) : Accumulator
{
    private object? _value;

    public override void Accumulate(object? value)
    {
        if (value == null)
        {
            return;
        }

        if (_value == null)
        {
            _value = value;
        }
        else
        {
            _value = value switch
            {
                int i => (int) _value + i,
                long l => (long) _value + l,
                double d => (double) _value + d,
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

    public override ScalarValue Evaluate =>
        DataType == ColumnDataType.Integer
            ? new IntegerScalar(Convert.ToInt64(_value))
            : new DoubleScalar(Convert.ToDouble(_value));
}