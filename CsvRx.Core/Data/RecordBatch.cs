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
}
