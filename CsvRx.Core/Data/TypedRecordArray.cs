using System.Collections;

namespace CsvRx.Core.Data;

public abstract class TypedRecordArray<T> : RecordArray
{
    public List<T> List { get; private set; } = new();

    public List<int> GetSortColumnIndices(bool ascending, int? start = null, int? take = null)
    {
        var skip = start ?? 0;
        var count = take ?? List.Count;

        // Only sort on the relevant fields within a group.  For a single
        // sort, the entire column is the group.  For subsequent columns
        // each sort is limited to the items in each parents distinct 
        // list of sorted values.  
        var groupSubset = List.Skip(skip).Take(count);//.ToList();
        // Get the original indexed position of the items in the list
        var indexMap = groupSubset.Select((value, index) => new KeyValuePair<T,int>(value, index));
        // Apply ascending or descending sort ordering
        var sorted = (ascending ? indexMap.OrderBy(_ => _.Key) : indexMap.OrderByDescending(_ => _.Key));
        // Get the index values as they should appear once rearranged in the sort operation
        var indices = sorted.Select(_ => _.Value);//.ToList();
        // Order the indices by their position in the sort and return the index at that position
        // e.g. as list with values
        // c, b, d, e, a has indices
        // 0, 1, 2, 3, 4 and should be finally sorted
        //
        // a, b, c, d, e
        // 4, 1, 0, 2, 3
        // This is the index order that needs to be applied to the array 
        // segment that will be sorted.  
        return indices.Select((p,i) => (Index: i, Position: p)).OrderBy(_=>_.Position).Select(_ => _.Index).ToList();
    }

    public override List<int> GetSortIndices(bool descending, int? start = null, int? take = null)
    {
        return GetSortColumnIndices(descending, start, take);
    }

    public override void Concat(IList values)
    {
        List.AddRange(values.Cast<T>());
    }

    public override void Reorder(List<int> indices)
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

    public override void Slice(int offset, int count)
    {
        List = List.Skip(offset).Take(count).ToList();
    }
}