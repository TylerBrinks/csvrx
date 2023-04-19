namespace CsvRx.Data;

public class Schema
{
    public Schema(List<Field> fields)
    {
        Fields = fields;
    }

    public List<Field> Fields { get; }

    //public Schema Select(List<string> names)
    //{
    //    var fields = new List<Field>();
    //    foreach (var name in names)
    //    {
    //        var m = Fields.FindAll(_ => _.Name == name);

    //        if (m.Count == 1)
    //        {
    //            fields.Add(m[0]);
    //        }
    //        else
    //        {
    //            throw new NotImplementedException();
    //        }
    //    }

    //    return new Schema(fields);
    //}

    //public Schema Project(List<int> indices)
    //{
    //    return new Schema(indices.Select((_, i) => Fields[i]).ToList());
    //}
    public Field? GetField(string name)
    {
        return Fields.FirstOrDefault(f => f.Name == name);
    }
}