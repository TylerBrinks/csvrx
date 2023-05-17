﻿using System.Collections;

namespace CsvRx.Core.Data;

public abstract class RecordArray
{
    public abstract void Add(object? value);

    public abstract List<int> GetSortIndices(bool ascending, int? start = null, int? take = null);

    public abstract void Concat(IList values);

    public abstract void Reorder(List<int> indices);

    public abstract void Slice(int offset, int count);

    public abstract IList Values { get; }

    public abstract RecordArray NewEmpty(int count);

    public virtual RecordArray FillWithNull(int count)
    {
        for (var i = 0; i < count; i++)
        {
            Add(null);
        }

        return this;
    }
}