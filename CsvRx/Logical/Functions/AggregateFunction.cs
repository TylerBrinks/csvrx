
namespace CsvRx.Logical.Functions
{
    internal record AggregateFunction
    {
        public static AggregateFunction? FromString(string name)
        {
            return name.ToLowerInvariant() switch
            {
                "min" => new MinFunction(),
                "max" => new MaxFunction(),
                _ => null
            };
        }
    }

    internal record MinFunction : AggregateFunction
    {
        public override string ToString()
        {
            return "MIN";
        }
    }

    internal record MaxFunction : AggregateFunction
    {
        public override string ToString()
        {
            return "MAX";
        }
    }
    internal record Count : AggregateFunction
    {
        public override string ToString()
        {
            return "COUNT";
        }
    }
}
