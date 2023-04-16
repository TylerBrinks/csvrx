namespace CsvRx.Logical;

public abstract record MathExpr(string Name, string Op, ILogicalExpression Left, ILogicalExpression Right) : BinaryExpr(Name, Op, Left, Right);