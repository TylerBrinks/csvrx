// ReSharper disable StringLiteralTypo
namespace CsvRx.Core.Logical.Expressions;

internal record AggregateFunction(
       AggregateFunctionType FunctionType,
       List<ILogicalExpression> Args,
       bool Distinct,
       ILogicalExpression? Filter = null) : ILogicalExpression
{
    internal static AggregateFunctionType? GetFunctionType(string name)
    {
        return name.ToLowerInvariant() switch
        {
            "min" => AggregateFunctionType.Min,
            "max" => AggregateFunctionType.Max,
            "count" => AggregateFunctionType.Count,
            "avg" or "mean" => AggregateFunctionType.Avg,
            "sum" => AggregateFunctionType.Sum,
            "median" => AggregateFunctionType.Median,
            
            //"approx_distinct" => AggregateFunctionType.ApproxDistinct,
            //"array_agg" => AggregateFunctionType.ArrayAgg

            "var" or "var_samp" => AggregateFunctionType.Variance,
            "var_pop" => AggregateFunctionType.VariancePop,
            "stddev" or "stddev_samp" => AggregateFunctionType.StdDev,
            "stddev_pop" => AggregateFunctionType.StdDevPop,
            "covar" or "covar_samp" => AggregateFunctionType.Covariance,
            "covar_pop" => AggregateFunctionType.CovariancePop,
            
            //"corr" => AggregateFunctionType.Correlation,
            //"approx_percentile_cont" => AggregateFunction::ApproxPercentileCont,
            //"approx_percentile_cont_with_weight" => {
            //"approx_median" => AggregateFunctionType.ApproxMedian,
            //"grouping" => AggregateFunction::Grouping,

            //TODO other aggregate functions
            _ => null
        };
    }

    public override string ToString()
    {
        var exp = string.Join(", ", Args.Select(_ => _.ToString()));
        return $"{FunctionType}({exp})";
    }

    public virtual bool Equals(AggregateFunction? other)
    {
        if (other == null) { return false; }

        var equal = FunctionType == other.FunctionType &&
                    Distinct == other.Distinct &&
                    Args.SequenceEqual(other.Args);

        if (equal && Filter != null)
        {
            equal &= Filter.Equals(other.Filter);
        }

        return equal;
    }
}

