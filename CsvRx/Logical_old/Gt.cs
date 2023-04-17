namespace CsvRx.Logical;

public record Gt(ILogicalExpression Left, ILogicalExpression Right) : BooleanBinaryExpr("gt", ">", Left, Right);