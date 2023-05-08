using CsvRx.Core.Data;
using CsvRx.Core.Values;
using SqlParser.Ast;

namespace CsvRx.Core.Physical.Expressions;

internal record Binary(IPhysicalExpression Left, BinaryOperator Op, IPhysicalExpression Right) : IPhysicalExpression
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

    private static ColumnDataType BitwiseCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        return (leftDataType, rightDataType) switch
        {
            (ColumnDataType.Double, ColumnDataType.Double)
                or (ColumnDataType.Double, ColumnDataType.Integer)
                or (ColumnDataType.Integer, ColumnDataType.Double)
                or (_, ColumnDataType.Double)
                or (ColumnDataType.Double, _) => ColumnDataType.Double,

            (ColumnDataType.Integer, ColumnDataType.Integer)
                or (ColumnDataType.Integer, _)
                or (_, ColumnDataType.Integer) => ColumnDataType.Integer,

            _ => throw new NotImplementedException("Coercion not implemented")
        };
    }

    private static ColumnDataType MathNumericalCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        //todo check both not null and both numeric

        return (leftDataType, rightDataType) switch
        {
            (ColumnDataType.Double, _)
                or (ColumnDataType.Double, _) => ColumnDataType.Double,

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

    private static ColumnDataType? StringCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        return leftDataType == ColumnDataType.Utf8 && rightDataType == ColumnDataType.Utf8
            ? ColumnDataType.Utf8
            : null;
    }

    private static ColumnDataType? ComparisonBinaryNumericCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        // todo check if non-numeric types
        if (leftDataType == rightDataType)
        {
            return leftDataType;
        }

        return (leftDataType, rightDataType) switch
        {
            (ColumnDataType.Double, ColumnDataType.Double)
                or (ColumnDataType.Integer, ColumnDataType.Double)
                or (ColumnDataType.Double, ColumnDataType.Integer)
                or (ColumnDataType.Double, _)
                or (_, ColumnDataType.Double) => ColumnDataType.Double,

            (ColumnDataType.Integer, ColumnDataType.Integer)
                or (ColumnDataType.Integer, _)
                or (_, ColumnDataType.Integer) => ColumnDataType.Integer,

            _ => null
        };
    }

    public ColumnValue Evaluate(RecordBatch batch, int? schemaIndex = null)
    {
        var leftValue = Left.Evaluate(batch);
        var rightValue = Right.Evaluate(batch);

        if (leftValue.Size != rightValue.Size)
        {
            throw new InvalidOperationException("size mismatch");
        }

        //todo check types
        //var leftDataType = leftValue.DataType;
        //var rightDataType = rightValue.DataType;

        //if (leftDataType != rightDataType)
        //{
        //    throw new InvalidOperationException("Data type mismatch");
        //}

        return Compare(leftValue, rightValue);
    }

    public BooleanColumnValue Compare(ColumnValue leftValue, ColumnValue rightValue)
    {
        var bitVector = new bool[leftValue.Size];

        for (var i = 0; i < leftValue.Size; i++)
        {
            var value = CompareValues(leftValue.GetValue(i), rightValue.GetValue(i), leftValue.DataType);
            bitVector[i] = value;
        }

        return new BooleanColumnValue(bitVector);
    }

    private bool CompareValues(object left, object right, ColumnDataType dataType)
    {
        switch (dataType)
        {
            case ColumnDataType.Utf8:
                return CompareStrings(left, right);

            case ColumnDataType.Integer:
                return CompareIntegers(left, right);

            case ColumnDataType.Double:
                return CompareDecimals(left, right);

            case ColumnDataType.Boolean:
                return CompareBooleans(left, right);

            default:
                throw new NotImplementedException("CompareValues data type not implemented");
        }
    }

    private bool CompareStrings(object left, object right)
    {
        var leftValue = (string)left;
        var rightValue = (string)right;

        return Op switch
        {
            BinaryOperator.Eq => leftValue == rightValue,
            BinaryOperator.NotEq => leftValue != rightValue,
            _ => throw new NotImplementedException("CompareStrings not implemented for strings yet")
        };
    }

    private bool CompareIntegers(object left, object right)
    {
        var leftValue = (long)left;
        var rightValue = (long)right;

        return Op switch
        {
            BinaryOperator.Eq => leftValue == rightValue,
            BinaryOperator.NotEq => leftValue != rightValue,
            BinaryOperator.Gt => leftValue > rightValue,
            BinaryOperator.Lt => leftValue < rightValue,
            BinaryOperator.GtEq => leftValue >= rightValue,
            BinaryOperator.LtEq => leftValue <= rightValue,

            _ => throw new NotImplementedException("CompareIntegers not implemented for integers yet")
        };
    }
   
    private bool CompareDecimals(object left, object right)
    {
        var leftValue = Convert.ToDecimal(left);
        var rightValue = Convert.ToDecimal(right);

        return Op switch
        {
            BinaryOperator.Eq => leftValue == rightValue,
            BinaryOperator.NotEq => leftValue != rightValue,
            BinaryOperator.Gt => leftValue > rightValue,
            BinaryOperator.Lt => leftValue < rightValue,
            BinaryOperator.GtEq => leftValue >= rightValue,
            BinaryOperator.LtEq => leftValue <= rightValue,

            _ => throw new NotImplementedException("CompareDecimals not implemented for integers yet")
        };
    }

    private bool CompareBooleans(object left, object right)
    {
        var leftValue = (bool)left;
        var rightValue = (bool)right;

        return Op switch
        {
            BinaryOperator.Eq => leftValue == rightValue,
            BinaryOperator.NotEq => leftValue != rightValue,
            BinaryOperator.And => leftValue && rightValue,
            BinaryOperator.Or => leftValue || rightValue,

            _ => throw new NotImplementedException("CompareBooleans not implemented for integers yet")
        };
    }

}