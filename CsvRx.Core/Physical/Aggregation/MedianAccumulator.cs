using CsvRx.Core.Data;
using CsvRx.Core.Logical.Values;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Aggregation;

internal record MedianAccumulator(ColumnDataType DataType) : Accumulator
{
    private readonly List<double> _values = new();

    public override void Accumulate(object? value)
    {
        if (value == null)
        {
            return;
        }

        _values.Add(Convert.ToDouble(value));
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

    public override object Value => CalculateMedian().RawValue!;

    public override List<ScalarValue> State => new() { Evaluate };

    public override ScalarValue Evaluate => CalculateMedian();

    private ScalarValue CalculateMedian()
    {
        var valueArray = _values.ToArray();
        Array.Sort(valueArray);
        var length = valueArray.Length;
        var middle = length / 2;

        var median = length % 2 != 0 
            ? valueArray[middle] 
            : (valueArray[middle] + valueArray[middle - 1]) / 2;

        return DataType switch
        {
            ColumnDataType.Integer => new IntegerScalar(Convert.ToInt64(median)),
            _ => new DoubleScalar(median),
        };
    }
}