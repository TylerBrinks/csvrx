using CsvRx.Data;

namespace CsvRx.Core.Logical.Expressions;

public abstract record ScalarValue(object? RawValue, ColumnDataType DataType)
{
    public override string ToString()
    {
        return RawValue == null ? "" : RawValue.ToString()!;
    }
}