using System.Collections;

namespace CsvRx.Core.Data;

internal class StringArray : TypedRecordArray<string?>
{
    public override void Add(object? s)
    {
        List.Add((string)s);
    }

    public override IList Values => List;
}