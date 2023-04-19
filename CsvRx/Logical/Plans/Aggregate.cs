using CsvRx.Data;

namespace CsvRx.Logical.Plans
{
    internal record Aggregate: ILogicalPlan
    {
        private readonly Schema _schema;

        public Aggregate(ILogicalPlan plan,
            List<ILogicalExpression> groupExpressions,
            List<ILogicalExpression> aggregateExpressions)
        {
            var aggregateProjectionExpressions = groupExpressions.Concat(aggregateExpressions).Select(e => e).ToList();
            //var fields = 
            //_schema = Schema();
        }

        public Schema Schema => _schema;
        
        public string ToStringIndented(Indentation? indentation)
        {
            return "Aggregate Plan";
        }
    }
}
