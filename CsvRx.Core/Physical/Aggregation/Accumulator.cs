using CsvRx.Core.Logical.Values;
using CsvRx.Core.Values;

namespace CsvRx.Core.Physical.Aggregation;

internal abstract record Accumulator
{
    public abstract void Accumulate(object value);

    public abstract void MergeBatch(List<ArrayColumnValue> values);
  
    public abstract void UpdateBatch(List<ArrayColumnValue> values);

    public abstract object? Value { get; }

    public abstract List<ScalarValue> State { get; }

    public abstract ScalarValue Evaluate { get; }
}