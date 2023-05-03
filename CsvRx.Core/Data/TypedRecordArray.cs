using System.Collections;

namespace CsvRx.Core.Data;

public abstract class TypedRecordArray<T> : RecordArray
{
    public List<T> List { get; } = new();

    public List<int> GetSortColumnIndices(bool descending, int? start = null, int? take = null)
    {
        var skip = start ?? 0;
        var cnt = take ?? List.Count;
        //var delta = upper - skip;

        var taken = List.Skip(skip).Take(cnt).ToList(); 
        var sorted = taken.Select((v,i) => new KeyValuePair<T,int>(v,i)).OrderBy(_ => _.Key).ToList();
        var indices = sorted.Select(_ => _.Value).ToList();
        return indices.Select((p,i) => (Index: i, Position: p)).OrderBy(_=>_.Position).Select(_ => _.Index).ToList();
    }

    public void ConcatValues(IList values)
    {
        List.AddRange(values.Cast<T>());
    }

    public void ReorderValues(List<int> indices)
    {
        // Clone the list since it will be reordered while
        // other arrays need the original list to reorder.
        var order = indices.ToList();

        var temp = new T[List.Count];

        for (var i = 0; i < List.Count; i++)
        {
            temp[order[i]] = List[i];
        }

        for (var i = 0; i < List.Count; i++)
        {
            List[i] = temp[i];
            order[i] = i;
        }
    }
}