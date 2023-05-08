using CsvRx.Core.Data;
using CsvRx.Core.Physical.Aggregation;

namespace CsvRx.Tests;

public class AccumulatorTests
{
    [Fact]
    public void MaxAccumulator_Compares_Values()
    {
        var accumulator = new MaxAccumulator();
        accumulator.Accumulate(-1);
        accumulator.Accumulate(1);
        accumulator.Accumulate(2);
        accumulator.Accumulate(3);
        accumulator.Accumulate(4.5);
        accumulator.Accumulate(5);

        Assert.Equal(5d, accumulator.Value);
    }

    [Fact]
    public void MinAccumulator_Compares_Values()
    {
        var accumulator = new MinAccumulator();
        accumulator.Accumulate(-1);
        accumulator.Accumulate(1);
        accumulator.Accumulate(2);
        accumulator.Accumulate(3);
        accumulator.Accumulate(4.5);
        accumulator.Accumulate(5);

        Assert.Equal(-1d, accumulator.Value);
        Assert.Equal(-1l, accumulator.Evaluate.RawValue);
    }

    [Fact]
    public void AverageAccumulator_Averages_Values()
    {
        var accumulator = new AverageAccumulator(ColumnDataType.Integer);
        accumulator.Accumulate(1);
        accumulator.Accumulate(2);
        accumulator.Accumulate(3);
        accumulator.Accumulate(4);
        accumulator.Accumulate(5);

        Assert.Equal(3d, accumulator.Value);
        Assert.Equal(3l, accumulator.Evaluate.RawValue);
    }

    [Fact]
    public void CountAccumulator_Averages_Values()
    {
        var accumulator = new CountAccumulator();

        for (var i = 0; i < 100; i++)
        {
            accumulator.Accumulate(i);
        }

        Assert.Equal((uint)100, accumulator.Value);
    }

    [Fact]
    public void SumAccumulator_Averages_Values()
    {
        var accumulator = new SumAccumulator(ColumnDataType.Integer);

        accumulator.Accumulate(1);
        accumulator.Accumulate(2);
        accumulator.Accumulate(3);
        accumulator.Accumulate(4);
        accumulator.Accumulate(5);

        Assert.Equal(15, accumulator.Value);
    }

    [Fact]
    public void MedianAccumulator_Finds_Middle_Value()
    {
        var accumulator = new MedianAccumulator(ColumnDataType.Integer);

        accumulator.Accumulate(7);
        accumulator.Accumulate(3);
        accumulator.Accumulate(2);
        accumulator.Accumulate(8);
        accumulator.Accumulate(5);
        accumulator.Accumulate(6);
        accumulator.Accumulate(1);
        accumulator.Accumulate(9);
        accumulator.Accumulate(4);

        Assert.Equal(5l, accumulator.Value);
        Assert.Equal(5l, accumulator.Evaluate.RawValue);
    }
}