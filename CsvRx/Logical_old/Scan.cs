using CsvRx.Data;

namespace CsvRx.Logical;

public class Scan : ILogicalPlan
{
    private readonly DataSource _dataSource;
    private readonly List<string> _projection;
    private readonly string _path;

    public Scan(string path, DataSource dataSource, List<string>? projection = null)
    {
        _path = path;
        _dataSource = dataSource;
        _projection = projection ?? new List<string>();
        Schema = DeriveSchema();
    }

    private Schema DeriveSchema()
    {
        var schema = _dataSource.Schema;
        return _projection.Any() ? schema.Select(_projection) : schema;
    }

    public Schema Schema { get; }

    public List<ILogicalPlan> Children()
    {
        return new List<ILogicalPlan>();
    }

    public string Path => _path;
    public List<string> Projection => _projection;
    public DataSource DataSource => _dataSource;
}