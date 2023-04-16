namespace CsvRx.Logical;

public record Add(ILogicalExpression Left, ILogicalExpression Right) : MathExpr("add", "+", Left, Right);