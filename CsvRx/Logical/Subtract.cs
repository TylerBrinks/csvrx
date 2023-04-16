namespace CsvRx.Logical;

public record Subtract(ILogicalExpression Left, ILogicalExpression Right) : MathExpr("subtract", "-", Left, Right);