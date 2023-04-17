namespace CsvRx.Logical;

public record Neq(ILogicalExpression Left, ILogicalExpression Right) : BooleanBinaryExpr("neq", "!=", Left, Right);