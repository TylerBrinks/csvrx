using CsvRx.Data;

namespace CsvRx.Core.Data
{
    public record RecordBatch
    {
        internal RecordBatch(Schema schema)
        {
            Schema = schema;

            foreach (var field in schema.Fields)
            {
                Results.Add(GetArrayType(field));
            }
        }

        private RecordArray GetArrayType(Field field)
        {
            return field.DataType switch
            {
                ColumnDataType.Utf8 => new StringArray(),
                ColumnDataType.Integer => new IntegerArray(),
                ColumnDataType.Boolean => new BooleanArray(),
                ColumnDataType.Decimal => new DecimalArray()
            };
        }

        public Schema Schema { get; }

        public List<RecordArray> Results { get; set; } = new();

        public int RowCount => Results.Count > 0 ? Results.First().Array.Count : 0;
    }
}
