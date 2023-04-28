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

