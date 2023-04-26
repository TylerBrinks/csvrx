using System.Collections;

namespace CsvRx.Core.Data;

internal class DecimalArray : TypedRecordArray<decimal?>
{
    public override void Add(object? s)
    {
        var parsed = decimal.TryParse(s.ToString(), out var result);
        if (parsed)
        {
            List.Add(result);
        }
        else
        {
            List.Add(null);
        }
    }
    public override IList Array => List;
}