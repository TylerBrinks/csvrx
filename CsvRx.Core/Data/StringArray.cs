using System.Collections;

namespace CsvRx.Core.Data;

internal class StringArray : TypedRecordArray<string?>
{
    public override void Add(object? s)
    {
        List.Add((string)s);
    }

    public override IList Values => List;
    
    public override List<int> GetSortIndices(bool descending, int? start = null, int? take = null)
    {
        return GetSortColumnIndices(descending, start, take);
    }

    public override void Concat(IList values)
    { 
        ConcatValues(values);
    }

    public override void Reorder(List<int> indices)
    {
        ReorderValues(indices);
    }
}