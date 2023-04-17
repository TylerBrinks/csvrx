namespace CsvRx.Logical;

public record Eq(ILogicalExpression Left, ILogicalExpression Right) : BooleanBinaryExpr("eq", "=", Left, Right);