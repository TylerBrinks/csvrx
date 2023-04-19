
namespace CsvRx.Logical.Functions
{
    internal enum AggregateFunctionType
    {
        Count,
        Sum,
        Min,
        Max,
        Avg,
        Median,
        /// Approximate aggregate function
        ApproxDistinct,
        ArrayAgg,
        /// Variance (Sample)
        Variance,
        /// Variance (Population)
        VariancePop,
        /// Standard Deviation (Sample)
        StdDev,
        /// Standard Deviation (Population)
        StdDevPop,
        /// Covariance (Sample)
        Covariance,
        /// Covariance (Population)
        CovariancePop,
        Correlation,
        /// Approximate continuous percentile function
        ApproxPercentileCont,
        /// Approximate continuous percentile function with weight
        ApproxPercentileContWithWeight,
        ApproxMedian,
        Grouping,
    }

    internal record AggregateFunction(
        AggregateFunctionType FunctionType,
        List<ILogicalExpression> Args,
        bool Distinct,
        ILogicalExpression? Filter = null) : ILogicalExpression
    {
        public static AggregateFunctionType? GetFunctionType(string name)
        {
            return name.ToLowerInvariant() switch
            {
                "min" => AggregateFunctionType.Min,
                "max" => AggregateFunctionType.Max,
                _ => null
            };
        }

        public override string ToString()
        {
            var exp = string.Join(", ", Args.Select(_ => _.ToString()));
            return $"{FunctionType}({exp})";
        }
    }

    //internal record MinFunction// : AggregateFunction
    //{
    //    public override string ToString()
    //    {
    //        return "MIN";
    //    }
    //}

    //internal record MaxFunction// : AggregateFunction
    //{
    //    public override string ToString()
    //    {
    //        return "MAX";
    //    }
    //}
    //internal record Count //: AggregateFunction
    //{
    //    public override string ToString()
    //    {
    //        return "COUNT";
    //    }
    //}
}
