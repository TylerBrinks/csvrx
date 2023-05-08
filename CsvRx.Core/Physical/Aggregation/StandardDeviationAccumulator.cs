using CsvRx.Core.Data;
using CsvRx.Core.Logical.Values;

namespace CsvRx.Core.Physical.Aggregation;

internal record StandardDeviationAccumulator(ColumnDataType DataType, StatisticType StatisticType) 
    : VarianceAccumulator(DataType, StatisticType)
{
    public override object? Value => CalculateStandardDeviation();

    public override ScalarValue Evaluate
    {
        get
        {
            var deviation = CalculateStandardDeviation();

            return DataType == ColumnDataType.Integer
                ? new IntegerScalar(Convert.ToInt64(deviation))
                : new DoubleScalar(deviation);
        }
    }

    private double CalculateStandardDeviation()
    {
        return Math.Sqrt(CalculateVariance());
    }
}