using CsvRx.Core.Data;

namespace CsvRx.Core.Values;

internal abstract record ColumnValue(ColumnDataType DataType)
{
    internal abstract int Size { get; }
    internal abstract object? GetValue(int index);
}