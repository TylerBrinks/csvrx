using System.Collections;

namespace CsvRx.Core.Data;

internal class StringArray : TypedRecordArray<string?>
{
    public override void Add(object? s)
    {
        if (s != null)
        {
            if (s is string str)
            {
                List.Add(str);
            }
            else
            {
                List.Add(s.ToString());
            }
        }
        else
        {
            List.Add(null);
        }
    }

    public override IList Values => List;

    public override RecordArray NewEmpty(int count)
    {
        return new StringArray().FillWithNull(count);
    }
}