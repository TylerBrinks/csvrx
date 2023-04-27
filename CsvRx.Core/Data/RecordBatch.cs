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

        private static RecordArray GetArrayType(Field field)
        {
            return field.DataType switch
            {
                ColumnDataType.Utf8 => new StringArray(),
                ColumnDataType.Integer => new IntegerArray(),
                ColumnDataType.Boolean => new BooleanArray(),
                ColumnDataType.Decimal => new DecimalArray(),
                _ => throw new NotSupportedException()
            };
        }

        public Schema Schema { get; }

        public List<RecordArray> Results { get; set; } = new();

        public int RowCount => Results.Count > 0 ? Results.First().Values.Count : 0;
    }
}
