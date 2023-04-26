namespace CsvRx.Data;

[Flags]
public enum ColumnDataType
{
    Boolean = 1 << 0,
    Integer = 1 << 1,
    Decimal = 1 << 2,
    Date32 = 1 << 3,
    TimestampSecond = 1 << 4,
    TimestampMillisecond = 1 << 5,
    TimestampMicrosecond = 1 << 6,
    TimestampNanosecond = 1 << 7,
    Utf8 = 1 << 8,
}