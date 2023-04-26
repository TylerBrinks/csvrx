using System.Collections;
using CsvRx.Data;

namespace CsvRx.Core.Data;

internal record ArrayColumnValue(IList Values, ColumnDataType DataType) : ColumnValue(DataType)
{
    internal override int Size => Values.Count;

    internal override object GetValue(int i)
    {
        return Values[i];
    }
}