namespace CsvRx.Core.Data;

internal abstract record ColumnValue(ColumnDataType DataType)
{
    internal abstract int Size { get; }
    internal abstract object? GetValue(int index);
}