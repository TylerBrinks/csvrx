namespace CsvRx.Logical.Functions;

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

