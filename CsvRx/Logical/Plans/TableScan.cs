using CsvRx.Data;

namespace CsvRx.Logical.Plans
{
    internal record TableScan(string Name, Schema Schema) : ILogicalPlan
    {
        // table name,
        // projection
        // filters

        public string ToStringIndented(Indentation? indentation)
        {
            return ToString();
        }

        public override string ToString()
        {
            return $"Table Scan: {Name}";
        }
    }
}
