using CsvRx.Core.Data;

namespace CsvRx.Core.Logical.Values;

public abstract record ScalarValue(object? RawValue, ColumnDataType DataType)
{
    public override string ToString()
    {
        return RawValue == null ? "" : RawValue.ToString()!;
    }
}