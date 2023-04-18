using SqlParser.Ast;

namespace CsvRx.Logical.Expressions;
internal record BinaryExpr(ILogicalExpression Left, BinaryOperator Op, ILogicalExpression Right) : ILogicalExpression
{
    public override string ToString()
    {
        return $"{Left} {Op.ToString().ToUpperInvariant()} {Right}";
    }
}
