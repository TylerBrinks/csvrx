using System.Collections;

namespace CsvRx.Core.Data;

internal class DoubleArray : TypedRecordArray<double?>
{
    public override void Add(object? s)
    {
        var parsed = double.TryParse(s?.ToString(), out var result);
        if (parsed)
        {
            List.Add(result);
        }
        else
        {
            List.Add(null);
        }
    }
    public override IList Values => List;

    public override RecordArray NewEmpty(int count)
    {
        var array = new DoubleArray();
        FillWithNull(array, count);
        return array;
    }
}