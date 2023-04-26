using CsvRx.Data;

namespace CsvRx.Core.Data;

internal abstract record ColumnValue(ColumnDataType DataType)
{
    internal abstract object GetValue(int index);
    internal abstract int Size { get; }
}