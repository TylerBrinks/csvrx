using CsvRx.Data;

namespace CsvRx.Logical
{
    internal record TableScan(string Name, DataSource DataSource) : ILogicalPlan
    {
        // table name,
        // projection
        // filters
        public Schema Schema => DataSource.Schema;
       
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
