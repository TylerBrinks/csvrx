namespace CsvRx.Logical;

public record Divide(ILogicalExpression Left, ILogicalExpression Right) : MathExpr("div", "/", Left, Right);