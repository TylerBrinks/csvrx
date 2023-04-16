namespace CsvRx.Logical;

public record LtEq(ILogicalExpression Left, ILogicalExpression Right) : BooleanBinaryExpr("lteq", "<=", Left, Right);