using System.Globalization;
using CsvHelper;

namespace CsvRx.Data;

public class CsvDataSource : DataSource
{
    private readonly string _filePath;
    private Schema? _schema;

    public CsvDataSource(string filePath, Schema? schema = null)
    {
        _filePath = filePath;
        if (schema != null)
        {
            _schema = schema;
        }
    }

    public Schema InferSchema()
    {
        using var reader = new StreamReader(_filePath);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
        csv.Read();
        csv.ReadHeader();
        var headers = csv.HeaderRecord;

        //while (csv.Read())
        //{
        //    for (var i = 0; i < header.Length; i++)
        //    {
        //        csv.GetField(i);
        //    }
        //}

        return new Schema(headers.Select(h => new Field(h)).ToList());
    }

    public override Schema Schema => _schema ??= InferSchema();
}