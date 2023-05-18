using System.Reflection.Metadata.Ecma335;
using CsvRx.Core.Logical.Values;

namespace CsvRx.Core.Logical.Expressions;

internal record Literal(ScalarValue Value) : ILogicalExpression
{
    public override string ToString()
    {
        return Value.ToString();
    }
}