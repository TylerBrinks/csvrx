namespace CsvRx.Logical;

public record Lt(ILogicalExpression Left, ILogicalExpression Right) : BooleanBinaryExpr("lt", "<", Left, Right);