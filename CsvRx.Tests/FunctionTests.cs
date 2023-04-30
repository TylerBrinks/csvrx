using CsvRx.Core.Data;
using CsvRx.Core.Logical.Values;
using CsvRx.Core.Physical.Aggregation;
using CsvRx.Core.Physical.Expressions;
using CsvRx.Core.Physical.Functions;

namespace CsvRx.Tests;

public class FunctionTests
{
    [Fact]
    public void Count_Function_Defaults()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new CountFunction(expression, "test", ColumnDataType.Utf8);
        Assert.Equal(1, fn.StateFields.Count);
        Assert.Equal("test[count]", fn.StateFields.First().Name);
        Assert.Equal(ColumnDataType.Utf8, fn.StateFields.First().DataType);
        Assert.Equal(new Field("test", ColumnDataType.Utf8), fn.Field);
        Assert.Equal(new Field("test", ColumnDataType.Utf8), fn.Field);
        Assert.Equal(expression, fn.Expressions.First());
    }

    [Fact]
    public void Count_Builds_Accumulator()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new CountFunction(expression, "test", ColumnDataType.Utf8);
        Assert.IsType<CountAccumulator>(fn.CreateAccumulator());
    }

    [Fact]
    public void Max_Function_Defaults()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new MaxFunction(expression, "test", ColumnDataType.Utf8);
        Assert.Equal(1, fn.StateFields.Count);
        Assert.Equal("test[max]", fn.StateFields.First().Name);
        Assert.Equal(ColumnDataType.Utf8, fn.StateFields.First().DataType);
        Assert.Equal(new Field("test", ColumnDataType.Utf8), fn.Field);
        Assert.Equal(new Field("test", ColumnDataType.Utf8), fn.Field);
        Assert.Equal(expression, fn.Expressions.First());
    }

    [Fact]
    public void Max_Builds_Accumulator()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new MaxFunction(expression, "test", ColumnDataType.Utf8);
        Assert.IsType<MaxAccumulator>(fn.CreateAccumulator());
    }

    [Fact]
    public void Min_Function_Defaults()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new MinFunction(expression, "test", ColumnDataType.Utf8);
        Assert.Equal(1, fn.StateFields.Count);
        Assert.Equal("test[min]", fn.StateFields.First().Name);
        Assert.Equal(ColumnDataType.Utf8, fn.StateFields.First().DataType);
        Assert.Equal(new Field("test", ColumnDataType.Utf8), fn.Field);
        Assert.Equal(new Field("test", ColumnDataType.Utf8), fn.Field);
        Assert.Equal(expression, fn.Expressions.First());
    }

    [Fact]
    public void Min_Builds_Accumulator()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new MinFunction(expression, "test", ColumnDataType.Utf8);
        Assert.IsType<MinAccumulator>(fn.CreateAccumulator());
    }

    [Fact]
    public void Sum_Function_Defaults()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new SumFunction(expression, "test", ColumnDataType.Utf8);
        Assert.Equal(1, fn.StateFields.Count);
        Assert.Equal("test[sum]", fn.StateFields.First().Name);
        Assert.Equal(ColumnDataType.Utf8, fn.StateFields.First().DataType);
        Assert.Equal(new Field("test", ColumnDataType.Utf8), fn.Field);
        Assert.Equal(new Field("test", ColumnDataType.Utf8), fn.Field);
        Assert.Equal(expression, fn.Expressions.First());
    }

    [Fact]
    public void Sum_Builds_Accumulator()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new SumFunction(expression, "test", ColumnDataType.Utf8);
        Assert.IsType<SumAccumulator>(fn.CreateAccumulator());
    }

    [Fact]
    public void Average_Function_Defaults()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new AverageFunction(expression, "test", ColumnDataType.Utf8);
        Assert.Equal(1, fn.StateFields.Count);
        Assert.Equal("test[avg]", fn.StateFields.First().Name);
        Assert.Equal(ColumnDataType.Utf8, fn.StateFields.First().DataType);
        Assert.Equal(new Field("test", ColumnDataType.Utf8), fn.Field);
        Assert.Equal(new Field("test", ColumnDataType.Utf8), fn.Field);
        Assert.Equal(expression, fn.Expressions.First());
    }

    [Fact]
    public void Average_Builds_Accumulator()
    {
        var expression = new Literal(new StringScalar("abc"));
        var fn = new AverageFunction(expression, "test", ColumnDataType.Utf8);
        Assert.IsType<AverageAccumulator>(fn.CreateAccumulator());
    }
}
