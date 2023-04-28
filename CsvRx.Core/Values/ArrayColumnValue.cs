using System.Collections;
using CsvRx.Core.Data;

namespace CsvRx.Core.Values;

internal record ArrayColumnValue(IList Values, ColumnDataType DataType) : ColumnValue(DataType)
{
    internal override int Size => Values.Count;

    internal override object GetValue(int i)
    {
        return Values[i];
    }
}