using CsvRx.Core.Logical;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Values;
using SqlParser.Ast;

namespace CsvRx.Tests.Logical
{
    public class LogicalExtensionsTests
    {
        [Fact]
        public void LogicalExpressions_Create_Names()
        {
            var column = new Column("column", new TableReference("table"));
            var alias = new Alias(column, "alias");
            var binary = new Binary(column, BinaryOperator.Eq, new Column("right"));
            var literal = new Literal(new StringScalar("value"));
            var function = new AggregateFunction(AggregateFunctionType.Min, 
                new List<ILogicalExpression> {column}, false);
            var functionDistinct = new AggregateFunction(AggregateFunctionType.Min,
                new List<ILogicalExpression> {column}, true);
            var wildcard = new Wildcard();

            Assert.Equal("table.column", column.CreateName());
            Assert.Equal("alias", alias.CreateName());
            Assert.Equal("table.column Eq right", binary.CreateName());
            Assert.Equal("value", literal.CreateName());
            Assert.Equal("MIN(table.column)", function.CreateName());
            Assert.Equal("MIN(DISTINCT table.column)", functionDistinct.CreateName());
            Assert.Equal("*", wildcard.CreateName());
        }

        [Fact]
        public void Expressions_Covert_To_Columns()
        {
            var expressions = new List<ILogicalExpression>
            {
                new Column("column"),
                new ScalarVariable(new []{"scalar"}),
                new Literal(new IntegerScalar(1))
            };

            var columns = new HashSet<Column>();
            expressions.ExpressionListToColumns(columns);

            Assert.Equal(2, columns.Count);
            Assert.Equal("column", columns.First().FlatName);
            Assert.Equal("scalar", columns.Last().FlatName);
        }
    }
}
