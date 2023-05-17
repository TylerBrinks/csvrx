using CsvRx.Core.Logical;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Values;

namespace CsvRx.Tests.Logical;

public class ExpressionTests
{
    [Fact]
    public void Column_Flattens_Name()
    {
        var column = new Column("column", new TableReference("table"));

        Assert.Equal("column", column.Name);
        Assert.Equal("table.column", column.FlatName);
    }

    [Fact]
    public void AggregateFunction_Gets_Function_From_Keywords()
    {
        Assert.Equal(AggregateFunctionType.Min, AggregateFunction.GetFunctionType("min"));
        Assert.Equal(AggregateFunctionType.Max, AggregateFunction.GetFunctionType("max"));
        Assert.Equal(AggregateFunctionType.Count, AggregateFunction.GetFunctionType("count"));
        Assert.Equal(AggregateFunctionType.Avg, AggregateFunction.GetFunctionType("avg"));
        Assert.Equal(AggregateFunctionType.Avg, AggregateFunction.GetFunctionType("mean"));
        Assert.Equal(AggregateFunctionType.Sum, AggregateFunction.GetFunctionType("sum"));
        Assert.Equal(AggregateFunctionType.Median, AggregateFunction.GetFunctionType("median"));
        Assert.Equal(AggregateFunctionType.Variance, AggregateFunction.GetFunctionType("var"));
        Assert.Equal(AggregateFunctionType.Variance, AggregateFunction.GetFunctionType("var_samp"));
        Assert.Equal(AggregateFunctionType.VariancePop, AggregateFunction.GetFunctionType("var_pop"));
        Assert.Equal(AggregateFunctionType.StdDev, AggregateFunction.GetFunctionType("stddev"));
        Assert.Equal(AggregateFunctionType.StdDev, AggregateFunction.GetFunctionType("stddev_samp"));
        Assert.Equal(AggregateFunctionType.StdDevPop, AggregateFunction.GetFunctionType("stddev_pop"));
        Assert.Equal(AggregateFunctionType.Covariance, AggregateFunction.GetFunctionType("covar"));
        Assert.Equal(AggregateFunctionType.Covariance, AggregateFunction.GetFunctionType("covar_samp"));
        Assert.Equal(AggregateFunctionType.CovariancePop, AggregateFunction.GetFunctionType("covar_pop"));
    }

    [Fact]
    public void AggregateFunction_Equality_Compares_Properties()
    {
        var fn1 = new AggregateFunction(AggregateFunctionType.Avg, new (){new Column("name")}, false, new Column("abc"));
        Assert.NotEqual(fn1, null);

        var fn2 = new AggregateFunction(AggregateFunctionType.Min, new (){new Column("nope")}, true, new Literal(new StringScalar("xyz")));

        Assert.NotEqual(fn1, fn2);

        fn2 = new AggregateFunction(AggregateFunctionType.Min, new() { new Column("nope") }, true, new Column("abc"));
        Assert.NotEqual(fn1, fn2);

        fn2 = new AggregateFunction(AggregateFunctionType.Min, new() { new Column("nope") }, false, new Column("abc"));
        Assert.NotEqual(fn1, fn2);

        fn2 = new AggregateFunction(AggregateFunctionType.Min, new() { new Column("name") }, false, new Column("abc"));
        Assert.NotEqual(fn1, fn2);

        fn2 = new AggregateFunction(AggregateFunctionType.Avg, new() { new Column("name") }, false, null);
        Assert.NotEqual(fn1, fn2);
       
        fn2 = new AggregateFunction(AggregateFunctionType.Avg, new() { new Column("name") }, false, new Column("abc"));
        Assert.Equal(fn1, fn2);
    }
}
