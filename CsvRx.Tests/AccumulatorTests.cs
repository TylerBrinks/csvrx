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

        Assert.Equal(5d, accumulator.Value);
    }
}