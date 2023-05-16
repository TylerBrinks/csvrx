using System.Collections;

namespace CsvRx.Core.Data;

internal class StringArray : TypedRecordArray<string?>
{
    public override void Add(object? s)
    {
        if (s != null)
        {
            List.Add((string) s);
        }
        else
        {
            List.Add(null);
        }
    }

    public override IList Values => List;

    public override RecordArray NewEmpty(int count)
    {
        var array = new StringArray();
        FillWithNull(count);
        return array;
    }
}