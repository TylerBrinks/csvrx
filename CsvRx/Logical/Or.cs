namespace CsvRx.Logical;

public record Or(ILogicalExpression Left, ILogicalExpression Right) : BooleanBinaryExpr("or", "OR", Left, Right);