using System.Collections;

namespace CsvRx.Core.Data;

internal class IntegerArray : TypedRecordArray<long?>
{
    public override void Add(object? s)
    {
        var parsed = long.TryParse(s?.ToString(), out var result);
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
}