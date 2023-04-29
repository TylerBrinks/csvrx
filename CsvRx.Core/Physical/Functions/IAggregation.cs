using CsvRx.Core.Physical.Aggregation;

namespace CsvRx.Core.Physical.Functions;

internal interface IAggregation
{
    Accumulator CreateAccumulator();
}