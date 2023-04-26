using System.Collections;
using CsvRx.Logical.Expressions;

namespace CsvRx.Data
{
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

    public abstract record ColumnValue(ColumnDataType DataType)
    {
        public abstract object GetValue(int index);
        public abstract int Size { get; }
    }

    public record ArrayColumnValue(IList Array, ColumnDataType DataType) : ColumnValue(DataType)
    {
        public override int Size => Array.Count;

        public override object GetValue(int i)
        {
            return Array[i];
        }
    }

    public record BooleanColumnValue(bool[] Values) : ColumnValue(ColumnDataType.Boolean)
    {
        public override int Size => Values.Length;

        public override object GetValue(int index)
        {
            return Values[index];
        }
    }

    public record ScalarColumnValue(ScalarValue Value, int RecordCount, ColumnDataType DataType) : ColumnValue(DataType)
    {
        public override int Size => RecordCount;

        public override object GetValue(int i)
        {
            return Value.RawValue;
        }
    }

    public abstract class RecordArray
    {
        public abstract void Add(object? value);
        public abstract IList Array { get; }
    }

    public abstract class TypedRecordArray<T> : RecordArray
    {
        public List<T> List { get; } = new();
    }

    public class StringArray : TypedRecordArray<string?>
    {
        public override void Add(object? s)
        {
            List.Add((string)s);
        }

        public override IList Array => List;
        //public override ColumnDataType DataType => ColumnDataType.Utf8;
    }

    public class IntegerArray : TypedRecordArray<int?>
    {
        public override void Add(object? s)
        {
            var parsed = int.TryParse(s.ToString(), out var result);
            if (parsed)
            {
                List.Add(result);
            }
            else
            {
                List.Add(null);
            }
        }
        public override IList Array => List;
        //public override ColumnDataType DataType => ColumnDataType.Integer;
    }

    public class BooleanArray : TypedRecordArray<bool?>
    {
        public override void Add(object? s)
        {
            var parsed = bool.TryParse(s.ToString(), out var result);
            if (parsed)
            {
                List.Add(result);
            }
            else
            {
                List.Add(null);
            }
        }
        public override IList Array => List;
        //public override ColumnDataType DataType => ColumnDataType.Boolean;
    }

    public class DecimalArray : TypedRecordArray<decimal?>
    {
        public override void Add(object? s)
        {
            var parsed = decimal.TryParse(s.ToString(), out var result);
            if (parsed)
            {
                List.Add(result);
            }
            else
            {
                List.Add(null);
            }
        }
        public override IList Array => List;
        //public override ColumnDataType DataType => ColumnDataType.Decimal;
    }
}
