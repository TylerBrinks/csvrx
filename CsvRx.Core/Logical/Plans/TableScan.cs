using CsvRx.Core.Data;

namespace CsvRx.Core.Logical.Plans;

internal record TableScan(string Name, Schema Schema, DataSource Source, List<int>? Projection = null) : ILogicalPlan
{
    public string ToStringIndented(Indentation? indentation)
    {
        return ToString();
    }

    public override string ToString()
    {
        return $"Table Scan: {Name}";
    }
}