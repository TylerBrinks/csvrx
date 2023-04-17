namespace CsvRx.Logical;

public abstract record BinaryExpr(string Name, string Op, ILogicalExpression Left, ILogicalExpression Right) 
    : MathExpression(Name, Op, Left, Right);