using System.Collections;

namespace CsvRx.Core.Data;

public abstract class RecordArray
{
    public abstract void Add(object? value);
    public abstract IList Array { get; }
}