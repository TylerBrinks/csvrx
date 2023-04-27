using SqlParser.Ast;

namespace CsvRx.Core.Logical.Expressions;

internal record BinaryExpr(LogicalExpression Left, BinaryOperator Op, LogicalExpression Right) : LogicalExpression
{
    public override string ToString()
    {
        return $"{Left} {Op.ToString().ToUpperInvariant()} {Right}";
    }
}
