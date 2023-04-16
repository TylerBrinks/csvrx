namespace CsvRx.Logical;

public record Multiply(ILogicalExpression Left, ILogicalExpression Right) : MathExpr("mult", "*", Left, Right);