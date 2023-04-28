using SqlParser.Ast;

namespace CsvRx.Core.Logical.Expressions;

internal record Binary(ILogicalExpression Left, BinaryOperator Op, ILogicalExpression Right) : ILogicalExpression
{
    public override string ToString()
    {
        return $"{Left} {Op.ToString().ToUpperInvariant()} {Right}";
    }
}
