namespace CsvRx.Logical;

public record GtEq(ILogicalExpression Left, ILogicalExpression Right) : BooleanBinaryExpr("gteq", ">=", Left, Right);