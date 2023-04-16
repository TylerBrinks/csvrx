namespace CsvRx.Logical;

public abstract record BooleanBinaryExpr(string Name, string Op, ILogicalExpression Left, ILogicalExpression Right)
    : BinaryExpr(Name, Op, Left, Right);