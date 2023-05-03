using System.Collections;

namespace CsvRx.Core.Data;

public abstract class RecordArray
{
    public abstract void Add(object? value);
    public abstract IList Values { get; }
    public abstract List<int> GetSortIndices(bool ascending, int? start = null, int? take = null);
    public abstract void Concat(IList values);
    public abstract void Reorder(List<int> indices);
}