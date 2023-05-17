using CsvRx.Core.Logical.Plans;
using System.Collections;

namespace CsvRx.Core.Data;

public record RecordBatch
{
    public RecordBatch(Schema schema)
    {
        Schema = schema;

        foreach (var field in schema.Fields)
        {
            Results.Add(GetArrayType(field));
        }
    }

    private static RecordArray GetArrayType(Field? field)
    {
        if (field != null)
        {
            return field.DataType switch
            {
                ColumnDataType.Utf8 => new StringArray(),
                ColumnDataType.Integer => new IntegerArray(),
                ColumnDataType.Boolean => new BooleanArray(),
                ColumnDataType.Double => new DoubleArray(),
                _ => new StringArray() //throw new NotSupportedException()
            };
        }

        return new StringArray();
    }

    public Schema Schema { get; }

    public List<RecordArray> Results { get; set; } = new();

    public int RowCount => Results.Count > 0 ? Results.First().Values.Count : 0;

    public void Reorder(List<int> indices, List<int>? columnsToIgnore = null)
    {
        for (var i = 0; i < Results.Count; i++)
        {
            var array = Results[i];

            if (columnsToIgnore != null && columnsToIgnore.Contains(i))
            {
                // Column is already sorted.
                continue;
            }

            array.Reorder(indices);
        }
    }

    public void Slice(int offset, int count)
    {
        if (offset + count > RowCount)
        {
            throw new IndexOutOfRangeException();
        }

        foreach (var array in Results)
        {
            array.Slice(offset, count);
        }
    }

    public IEnumerable<RecordBatch> Repartition(int batchSize)
    {
        var rowCount = RowCount;
        var count = 0;

        var partition = new RecordBatch(Schema);

        for (var i = 0; i < rowCount; i++)
        {
            for (var j = 0; j < Results.Count; j++)
            {
                partition.Results[j].Add(Results[j].Values[i]);
            }

            count++;

            if (count != batchSize){ continue; }

            yield return partition;

            partition = new RecordBatch(Schema);
            count = 0;
        }

        if (count > 0)
        {
            yield return partition;
        }
    }

    public static RecordBatch TryNew(Schema schema, List<object?> columns)
    {
        if (schema.Fields.Count != columns.Count)
        {
            throw new InvalidOperationException("Number of columns must match the number of fields");
        }

        var batch = new RecordBatch(schema);

        for (var i = 0; i < columns.Count; i++)
        {
            batch.Results[i].Add(columns[i]);
        }

        return batch;
    }

    public static RecordBatch TryNewWithLists(Schema schema, List<IList> columns)
    {
        var batch = new RecordBatch(schema);

        for (var i = 0; i < columns.Count; i++)
        {
            foreach (var value in columns[i])
            {
                batch.Results[i].Add(value);
            }
        }

        return batch;
    }

    public void Concat(RecordBatch leftBatch)
    {
        for (var i = 0; i < Results.Count; i++)
        {
            Results[i].Concat(leftBatch.Results[i].Values);
        }
    }
}
