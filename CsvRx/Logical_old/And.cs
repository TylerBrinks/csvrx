namespace CsvRx.Logical;

public record And(ILogicalExpression Left, ILogicalExpression Right) : BooleanBinaryExpr("and", "AND", Left, Right);