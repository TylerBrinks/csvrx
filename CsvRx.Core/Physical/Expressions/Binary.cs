using CsvRx.Core.Data;
using CsvRx.Core.Values;
using SqlParser.Ast;
using System.Collections;
using CsvRx.Core.Logical;

namespace CsvRx.Core.Physical.Expressions;

internal record Binary(IPhysicalExpression Left, BinaryOperator Op, IPhysicalExpression Right) : IPhysicalExpression
{
    public ColumnDataType GetDataType(Schema schema)
    {
        return LogicalExtensions.GetBinaryDataType(Left.GetDataType(schema), Op, Right.GetDataType(schema));
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

        return Op switch
        {
            BinaryOperator.Lt
                or BinaryOperator.LtEq
                or BinaryOperator.Gt
                or BinaryOperator.GtEq
                or BinaryOperator.Eq
                or BinaryOperator.NotEq => Compare(leftValue, rightValue),

            BinaryOperator.Plus => Add(leftValue, rightValue),
            BinaryOperator.Minus => Subtract(leftValue, rightValue),
            BinaryOperator.Multiply => Multiple(leftValue, rightValue),
            BinaryOperator.Divide => Divide(leftValue, rightValue),
            BinaryOperator.Modulo => Modulo(leftValue, rightValue),

            //TODO bitwise operators

            _ => throw new NotImplementedException("Binary Evaluation not yet implemented")
        };
    }

    public static ArrayColumnValue Calculate(ColumnValue leftValue, ColumnValue rightValue, Func<double, double, double> calculate)
    {
        var results = new double[leftValue.Size];

        for (var i = 0; i < leftValue.Size; i++)
        {
            var left = leftValue.GetValue(i);
            var right = rightValue.GetValue(i);

            var value = calculate(Convert.ToDouble(left), Convert.ToDouble(right));
            
            results[i] = value;
        }

        var outputType = LogicalExtensions.GetMathNumericalCoercion(leftValue.DataType, rightValue.DataType);

        IList data = outputType == ColumnDataType.Integer
            ? results.Select(Convert.ToInt64).ToList()
            : results.ToList();

        return new ArrayColumnValue(data, outputType);
    }

    public static ArrayColumnValue Add(ColumnValue leftValue, ColumnValue rightValue)
    {
        return Calculate(leftValue, rightValue, (l, r) => l + r);
    }

    public static ArrayColumnValue Subtract(ColumnValue leftValue, ColumnValue rightValue)
    {
        return Calculate(leftValue, rightValue, (l, r) => l - r);
    }

    public static ArrayColumnValue Multiple(ColumnValue leftValue, ColumnValue rightValue)
    {
        return Calculate(leftValue, rightValue, (l, r) => l * r);

    }

    public static ArrayColumnValue Divide(ColumnValue leftValue, ColumnValue rightValue)
    {
        return Calculate(leftValue, rightValue, (l, r) => l / r);
    }

    public static ArrayColumnValue Modulo(ColumnValue leftValue, ColumnValue rightValue)
    {
        return Calculate(leftValue, rightValue, (l, r) => l % r);
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

    private bool CompareValues(object? left, object? right, ColumnDataType dataType)
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

    private bool CompareStrings(object? left, object? right)
    {
        var leftValue = left as string ?? (left ?? "").ToString();
        var rightValue = right as string ?? (right ?? "").ToString();

        return Op switch
        {
            BinaryOperator.Eq => leftValue == rightValue,
            BinaryOperator.NotEq => leftValue != rightValue,
            _ => throw new NotImplementedException("CompareStrings not implemented for strings yet")
        };
    }

    private bool CompareIntegers(object? left, object? right)
    {
        var leftValue = Convert.ToInt64(left);
        var rightValue = Convert.ToInt64(right);

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

    private bool CompareDecimals(object? left, object? right)
    {
        var leftValue = Convert.ToDouble(left ?? double.NaN);
        var rightValue = Convert.ToDouble(right ?? double.NaN);

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

    private bool CompareBooleans(object? left, object? right)
    {
        var leftValue = Convert.ToBoolean(left ?? false);
        var rightValue = Convert.ToBoolean(right ?? false);

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