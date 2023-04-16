namespace CsvRx.Logical;

public record Modulus(ILogicalExpression Left, ILogicalExpression Right) : MathExpr("mod", "%", Left, Right);