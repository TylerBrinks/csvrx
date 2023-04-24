using CsvRx.Data;
using SqlParser.Ast;

namespace CsvRx.Physical.Expressions;

public record PhysicalBinaryExpr(IPhysicalExpression Left, BinaryOperator Op, IPhysicalExpression Right) : IPhysicalExpression
{
    public ColumnDataType GetDataType(Schema schema)
    {
        var resultType = CoerceTypes(Left.GetDataType(schema), Right.GetDataType(schema));

        return Op switch
        {
            BinaryOperator.Eq 
                or BinaryOperator.NotEq
                or BinaryOperator.And
                or BinaryOperator.Or
                or BinaryOperator.Lt
                or BinaryOperator.Gt
                or BinaryOperator.GtEq
                or BinaryOperator.LtEq => ColumnDataType.Boolean,

            BinaryOperator.BitwiseAnd
                or BinaryOperator.BitwiseOr
                or BinaryOperator.BitwiseXor => resultType,

            BinaryOperator.Plus
                or BinaryOperator.Minus
                or BinaryOperator.Divide
                or BinaryOperator.Multiply
                or BinaryOperator.Modulo => resultType,

            _ => throw new NotImplementedException($"Binary operator {Op} not implemented")
        };
    }

    private ColumnDataType CoerceTypes(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        return Op switch
        {
            BinaryOperator.BitwiseAnd
                or BinaryOperator.BitwiseOr
                or BinaryOperator.BitwiseXor
                or BinaryOperator.PGBitwiseShiftLeft
                or BinaryOperator.PGBitwiseShiftRight => BitwiseCoercion(leftDataType, rightDataType),

            BinaryOperator.And
                or BinaryOperator.Or
                when leftDataType is ColumnDataType.Boolean &&
                     rightDataType is ColumnDataType.Boolean => ColumnDataType.Boolean,

            BinaryOperator.Eq
                or BinaryOperator.NotEq
                or BinaryOperator.Lt
                or BinaryOperator.Gt
                or BinaryOperator.GtEq
                or BinaryOperator.LtEq => ComparisonCoercion(leftDataType, rightDataType),

            BinaryOperator.Plus
                or BinaryOperator.Minus
                or BinaryOperator.Divide
                or BinaryOperator.Multiply
                or BinaryOperator.Modulo => MathNumericalCoercion(leftDataType, rightDataType),

            _ => throw new NotImplementedException($"Column data type coercion not implemented for operator {Op}")
        };
    }

    private ColumnDataType BitwiseCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        return (leftDataType, rightDataType) switch
        {
            (ColumnDataType.Decimal, ColumnDataType.Decimal)
                or (ColumnDataType.Decimal, ColumnDataType.Integer)
                or (ColumnDataType.Integer, ColumnDataType.Decimal)
                or (_, ColumnDataType.Decimal)
                or (ColumnDataType.Decimal, _) => ColumnDataType.Decimal,

            (ColumnDataType.Integer, ColumnDataType.Integer)
                or (ColumnDataType.Integer, _)
                or (_, ColumnDataType.Integer) => ColumnDataType.Integer,

            _ => throw new NotImplementedException("Coercion not implemented")
        };
    }

    private ColumnDataType MathNumericalCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        //todo check both not null and both numeric

        return (leftDataType, rightDataType) switch
        {
            (ColumnDataType.Decimal, _ )
                or (ColumnDataType.Decimal, _) => ColumnDataType.Decimal,

            (ColumnDataType.Integer, _)
                or (ColumnDataType.Integer, _) => ColumnDataType.Integer,

            _ => throw new NotImplementedException("Coercion not implemented")
        };
    }

    private ColumnDataType ComparisonCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        if (leftDataType == rightDataType)
        {
            return leftDataType;
        }

        return ComparisonBinaryNumericCoercion(leftDataType, rightDataType) ??
               //DictionaryCoercion() ??
               //temporal
               StringCoercion(leftDataType, rightDataType) ??
               // null coercion
               //StringNumericCoercion()
               throw new NotImplementedException("Coercion not implemented");
    }

    private ColumnDataType? StringCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        return leftDataType == ColumnDataType.Utf8 && rightDataType == ColumnDataType.Utf8 
            ? ColumnDataType.Utf8 
            : null;
    }

    private ColumnDataType? ComparisonBinaryNumericCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        // todo check if non-numeric types
        if (leftDataType == rightDataType)
        {
            return leftDataType;
        }

        return (leftDataType, rightDataType) switch
        {
            (ColumnDataType.Decimal, ColumnDataType.Decimal)
                or (ColumnDataType.Integer, ColumnDataType.Decimal)
                or (ColumnDataType.Decimal, ColumnDataType.Integer)
                or (ColumnDataType.Decimal, _)
                or (_, ColumnDataType.Decimal) => ColumnDataType.Decimal,

            (ColumnDataType.Integer, ColumnDataType.Integer)
                or (ColumnDataType.Integer, _)
                or (_, ColumnDataType.Integer) => ColumnDataType.Integer,

            _ => null
        };
    }
}