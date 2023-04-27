namespace CsvRx.Core.Logical.Functions;

internal record AggregateFunction(
       AggregateFunctionType FunctionType,
       List<LogicalExpression> Args,
       bool Distinct,
       LogicalExpression? Filter = null) : LogicalExpression
{
    internal static AggregateFunctionType? GetFunctionType(string name)
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

    public virtual bool Equals(AggregateFunction? other)
    {
        if (other == null) { return false; }

        return FunctionType == other.FunctionType &&
               Distinct == other.Distinct &&
               Filter == other.Filter &&
               Args.SequenceEqual(other.Args);
    }
}

