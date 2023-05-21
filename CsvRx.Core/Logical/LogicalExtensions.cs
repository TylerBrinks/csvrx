using System.Runtime.InteropServices;
using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;
using CsvRx.Core.Logical.Values;
using CsvRx.Core.Physical.Expressions;
using SqlParser.Ast;
using static SqlParser.Ast.Expression;
using Aggregate = CsvRx.Core.Logical.Plans.Aggregate;
using Binary = CsvRx.Core.Logical.Expressions.Binary;
using Column = CsvRx.Core.Logical.Expressions.Column;
using Join = CsvRx.Core.Logical.Plans.Join;
using Literal = CsvRx.Core.Logical.Expressions.Literal;

namespace CsvRx.Core.Logical;

internal static class LogicalExtensions
{
    #region Logical Expression
    /// <summary>
    /// Creates a name for the underlying expression based on the
    /// expression's type and (optionally) contained value
    /// </summary>
    /// <param name="expression">Logical expression</param>
    /// <returns>Expression name</returns>
    /// <exception cref="NotImplementedException"></exception>
    internal static string CreateName(this ILogicalExpression expression)
    {
        return (expression switch
        {
            Alias a => a.Name,
            Column c => c.FlatName,
            Binary b => $"{b.Left.CreateName()} {b.Op} {b.Right.CreateName()}",
            AggregateFunction fn => GetFunctionName(fn, fn.Distinct, fn.Args),
            Literal { Value.RawValue: { } } l => l.Value.RawValue.ToString(),
            //Like
            // Case
            // cast
            // not
            // is null
            // is not null
            Wildcard => "*",
            _ => throw new NotImplementedException("need to implement")
        })!;

        static string GetFunctionName(AggregateFunction fn, bool distinct, IEnumerable<ILogicalExpression> args)
        {
            var names = args.Select(CreateName).ToList();
            var distinctName = distinct ? "DISTINCT " : string.Empty;
            var functionName = fn.FunctionType.ToString().ToUpperInvariant();

            return $"{functionName}({distinctName}{string.Join(",", names)})";
        }
    }
    /// <summary>
    /// Iterates a list of expressions and appends unique found across
    /// all expressions to a given HashSet
    /// </summary>
    /// <param name="expressions">Logical expressions to iterate and convert to columns</param>
    /// <param name="accumulator">HashSet to append with unique columns</param>
    internal static void ExpressionListToColumns(this List<ILogicalExpression> expressions, HashSet<Column> accumulator)
    {
        foreach (var expr in expressions)
        {
            expr.ExpressionToColumns(accumulator);
        }
    }
    /// <summary>
    /// Converts a logical expression into one or more columns.  A single expression
    /// may yield multiple columns.  A Binary expression, for example, may compare
    /// one column to another and yield a pair of columns
    /// </summary>
    /// <param name="expression">Logical expressions to convert</param>
    /// <param name="accumulator">HashSet to append with unique columns</param>
    internal static void ExpressionToColumns(this ILogicalExpression expression, HashSet<Column> accumulator)
    {
        expression.Apply(expr =>
        {
            try
            {
                Inspect((ILogicalExpression)expr);

                return VisitRecursion.Continue;
            }
            catch
            {
                return VisitRecursion.Stop;
            }
        });

        void Inspect(ILogicalExpression expr)
        {
            switch (expr)
            {
                case Column col:
                    accumulator.Add(col);
                    break;

                case ScalarVariable sv:
                    accumulator.Add(new Column(string.Join(".", sv.Names)));
                    break;
            }
        }
    }
    /// <summary>
    /// Convenience method to extract columns without needing an external hash set
    /// for every call to ExpressionToColumns
    /// </summary>
    /// <param name="expression">Expression to query</param>
    /// <returns>Hash set containing the expression's columns</returns>
    internal static HashSet<Column> ToColumns(this ILogicalExpression expression)
    {
        var columns = new HashSet<Column>();

        expression.ExpressionToColumns(columns);

        return columns;
    }
    /// <summary>
    /// Converts a list of expressions into a list of qualified fields
    /// </summary>
    /// <param name="expressions">Logical expressions to iterate</param>
    /// <param name="plan">Plan containing field definitions</param>
    /// <returns>List of qualified fields</returns>
    internal static List<QualifiedField> ExpressionListToFields(this IEnumerable<ILogicalExpression> expressions, ILogicalPlan plan)
    {
        return expressions.Select(e => e.ToField(plan.Schema)).ToList();
    }
    /// <summary>
    /// Converts a logical expression into a qualified field
    /// </summary>
    /// <param name="expression">Logical expression to convert into a field</param>
    /// <param name="schema">Schema containing the underlying field</param>
    /// <returns>Qualified field</returns>
    internal static QualifiedField ToField(this ILogicalExpression expression, Schema schema)
    {
        var dataType = expression.GetDataType(schema);

        if (expression is Column { Relation: { } } c)
        {
            return new QualifiedField(c.Name, dataType, c.Relation!);
        }

        return QualifiedField.Unqualified(expression.CreateName(), dataType);
    }
    /// <summary>
    /// Gets the data type for a given logical expression
    /// </summary>
    /// <param name="expression">Logical expression to interrogate</param>
    /// <param name="schema">Schema containing the underlying field and data type</param>
    /// <returns>Expression's return data type</returns>
    /// <exception cref="NotImplementedException"></exception>
    /// <exception cref="InvalidOperationException"></exception>
    internal static ColumnDataType GetDataType(this ILogicalExpression expression, Schema schema)
    {
        return expression switch
        {
            Column c => schema.GetField(c.Name)!.DataType,
            Alias a => a.Expression.GetDataType(schema),
            AggregateFunction fn => GetAggregateDataType(fn),
            Binary b => GetBinaryDataType(GetDataType(b.Left, schema), b.Op, GetDataType(b.Right, schema)),

            _ => throw new NotImplementedException("GetDataType not implemented for ColumnDataType"),
        };

        ColumnDataType GetAggregateDataType(AggregateFunction function)
        {
            var dataTypes = function.Args.Select(e => e.GetDataType(schema)).ToList();

            return function.FunctionType switch
            {
                AggregateFunctionType.Min or AggregateFunctionType.Max => CoercedTypes(function, dataTypes),
                AggregateFunctionType.Sum or AggregateFunctionType.Count => ColumnDataType.Integer,
                AggregateFunctionType.Avg
                    or AggregateFunctionType.Median
                    or AggregateFunctionType.StdDev
                    or AggregateFunctionType.StdDevPop
                    or AggregateFunctionType.Variance
                    or AggregateFunctionType.VariancePop
                    or AggregateFunctionType.Covariance
                    or AggregateFunctionType.CovariancePop
                    => ColumnDataType.Double,

                _ => throw new NotImplementedException("GetAggregateDataType need to implement"),
            };
        }

        ColumnDataType CoercedTypes(AggregateFunction function, IReadOnlyList<ColumnDataType> inputTypes)
        {
            return function.FunctionType switch
            {
                AggregateFunctionType.Min or AggregateFunctionType.Max => GetMinMaxType(),
                _ => throw new NotImplementedException("CoercedTypes need to implement"),
            };

            ColumnDataType GetMinMaxType()
            {
                if (inputTypes.Count != 1)
                {
                    throw new InvalidOperationException();
                }

                return inputTypes[0];
            }
        }
    }

    internal static ColumnDataType GetBinaryDataType(ColumnDataType leftDataType, BinaryOperator op, ColumnDataType rightDataType)
    {
        //var leftDataType = GetDataType(binary.Left, schema);
        //var rightDataType = GetDataType(binary.Left, schema);

        var resultType = CoerceBinaryTypes(leftDataType, op, rightDataType);

        return op switch
        {
            BinaryOperator.Eq
                or BinaryOperator.NotEq
                or BinaryOperator.And
                or BinaryOperator.Or
                or BinaryOperator.Lt
                or BinaryOperator.Gt
                or BinaryOperator.GtEq
                or BinaryOperator.LtEq
                // or BinaryOperator.RegexMatch
                // or BinaryOperator.RegexIMatch
                // or BinaryOperator.RegexNotMatch
                // or BinaryOperator.RegexNotIMatch
                // or BinaryOperator.IsDistinctFrom
                // or BinaryOperator.IsNotDistinctFrom
                => ColumnDataType.Boolean,

            _ => resultType
        };
    }

    internal static ColumnDataType CoerceBinaryTypes(ColumnDataType leftDataType, BinaryOperator op, ColumnDataType rightDataType)
    {
        switch (op)
        {
            case BinaryOperator.BitwiseAnd
                or BinaryOperator.BitwiseOr
                or BinaryOperator.BitwiseXor:
                return GetBitwiseCoercion(leftDataType, rightDataType);

            //case BinaryOperator.And or BinaryOperator.Or:
            //    return ColumnDataType.Boolean;
            case BinaryOperator.And or BinaryOperator.Or
                when leftDataType is ColumnDataType.Boolean &&
                     rightDataType is ColumnDataType.Boolean:
                return ColumnDataType.Boolean;

            case BinaryOperator.Eq
                or BinaryOperator.NotEq
                or BinaryOperator.Lt
                or BinaryOperator.Gt
                or BinaryOperator.LtEq
                or BinaryOperator.GtEq:
                return GetComparisonCoercion(leftDataType, rightDataType);

            //case BinaryOperator.Plus
            //    or BinaryOperator.Minus when IsDateOrTime(leftDataType) && IsDateOrTime(rightDataType):
            //    return TemporalAddSubCoercion();

            case BinaryOperator.Plus
                or BinaryOperator.Minus
                or BinaryOperator.Modulo
                or BinaryOperator.Divide
                or BinaryOperator.Multiply:
                return GetMathNumericalCoercion(leftDataType, rightDataType);

            case BinaryOperator.StringConcat:
            default:
                return ColumnDataType.Utf8;
        }
    }

    /// <summary>
    /// Finds the coerced data type for a bitwise operation
    /// </summary>
    /// <param name="leftDataType">Left hand data type</param>
    /// <param name="rightDataType">Right hand data type</param>
    /// <returns>Coerced column data type</returns>
    internal static ColumnDataType GetBitwiseCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        if (!leftDataType.IsNumeric() && !rightDataType.IsNumeric())
        {
            return ColumnDataType.Utf8;
        }

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
    /// <summary>
    /// Finds the coerced data type for a comparison operation
    /// </summary>
    /// <param name="leftDataType">Left hand data type</param>
    /// <param name="rightDataType">Right hand data type</param>
    /// <returns>Coerced column data type</returns>
    internal static ColumnDataType GetComparisonCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
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
    internal static ColumnDataType? StringCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        return leftDataType == ColumnDataType.Utf8 && rightDataType == ColumnDataType.Utf8
            ? ColumnDataType.Utf8
            : null;
    }
    internal static ColumnDataType? ComparisonBinaryNumericCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
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
    //internal static ColumnDataType GetComparisonCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    //{
    //    if (leftDataType == ColumnDataType.Boolean && rightDataType == ColumnDataType.Boolean)
    //    {
    //        return ColumnDataType.Boolean;
    //    }

    //    return ColumnDataType.Double;
    //}
    /// <summary>
    /// Finds the coerced data type for a math operation
    /// </summary>
    /// <param name="leftDataType">Left hand data type</param>
    /// <param name="rightDataType">Right hand data type</param>
    /// <returns>Coerced column data type</returns>
    internal static ColumnDataType GetMathNumericalCoercion(ColumnDataType leftDataType, ColumnDataType rightDataType)
    {
        if (!leftDataType.IsNumeric() && !rightDataType.IsNumeric())
        {
            return ColumnDataType.Utf8;
        }

        return (leftDataType, rightDataType) switch
        {
            (ColumnDataType.Double, _)
                or (_, ColumnDataType.Double) => ColumnDataType.Double,

            (ColumnDataType.Integer, _)
                or (_, ColumnDataType.Integer) => ColumnDataType.Integer,

            _ => throw new NotImplementedException("Coercion not implemented")
        };
    }
    /// <summary>
    /// Checks if a column data type is numeric
    /// </summary>
    /// <param name="dataType">Data type to evaluate</param>
    /// <returns>True if numeric; otherwise false.</returns>
    private static bool IsNumeric(this ColumnDataType dataType)
    {
        return dataType is ColumnDataType.Integer or ColumnDataType.Double;
    }
    // <summary>
    // Checks if a column data type is a date/time field
    // </summary>
    // <param name="dataType">Data type to evaluate</param>
    // <returns>True if date/time; otherwise false.</returns>
    //private static bool IsDateOrTime(this ColumnDataType dataType)
    //{
    //    return dataType is ColumnDataType.Date32
    //        or ColumnDataType.TimestampSecond
    //        or ColumnDataType.TimestampMillisecond
    //        or ColumnDataType.TimestampMicrosecond
    //        or ColumnDataType.TimestampNanosecond;
    //}
    /// <summary>
    /// Removes an alias and reverts to the underlying field name
    ///  for an expression that has been aliased
    /// </summary>
    /// <param name="expression"></param>
    /// <returns></returns>
    // ReSharper disable once IdentifierTypo
    internal static ILogicalExpression Unalias(this ILogicalExpression expression)
    {
        return expression is not Alias alias ? expression : alias.Expression;
    }
    /// <summary>
    /// Returns a cloned expression, but any of the expressions in the tree may be
    /// replaced or customized by the replacement function.
    ///
    /// The replace function is called repeatedly with expression, starting with
    /// the argument, then descending depth-first through its
    /// descendants. The function chooses to replace or keep (clone) each expression.
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="replacement"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    internal static ILogicalExpression CloneWithReplacement(this ILogicalExpression expression,
        Func<ILogicalExpression, ILogicalExpression?> replacement)
    {
        var replacementOpt = replacement(expression);

        if (replacementOpt != null)
        {
            return replacementOpt;
        }

        return expression switch
        {
            Column or Literal => expression,
            AggregateFunction fn => fn with { Args = fn.Args.Select(_ => _.CloneWithReplacement(replacement)).ToList() },
            Alias a => a with { Expression = a.Expression.CloneWithReplacement(replacement) },
            Binary b => new Binary(b.Left.CloneWithReplacement(replacement), b.Op, b.Right.CloneWithReplacement(replacement)),

            _ => throw new NotImplementedException() //todo other types
        };
    }
    /// <summary>
    /// Resolve all columns in the expression tree
    /// </summary>
    /// <param name="expression">Expression being resolved</param>
    /// <param name="schema">Schema containing fields being resolved</param>
    /// <returns>Resolved schema</returns>
    /// <exception cref="NotImplementedException"></exception>
    public static ILogicalExpression ResolveColumns(ILogicalExpression expression, Schema schema)
    {
        return expression.CloneWithReplacement(nested => nested is not Column c 
            ? null 
            : schema.GetFieldFromColumn(c)!.QualifiedColumn());
    }
    /// <summary>
    /// Relational expression from sql expression
    /// </summary>
    internal static ILogicalExpression SqlToExpression(this Expression? predicate, Schema schema, PlannerContext context)
    {
        var expr = SqlExpressionToLogicalExpression(predicate, schema, context);

        //TODO ?? rewrite qualifier
        //  ?? validate
        //  ?? infer

        return expr;
    }
    /// <summary>
    /// Rebuilds an expression as a projection on top of
    /// a collection of expressions.
    ///
    /// "a + b = 1" would require 2 individual input columns
    /// for 'a' and 'b'.  However if the base expressions
    /// already contain the "a + b" result, then that can
    /// be used in place of the columns.
    /// </summary>
    /// <param name="expression">Expression to rebase</param>
    /// <param name="baseExpressions">Base expressions</param>
    /// <param name="schema">Schema to search for replacement columns</param>
    /// <returns></returns>
    internal static ILogicalExpression RebaseExpression(
        this ILogicalExpression expression,
        ICollection<ILogicalExpression> baseExpressions,
        Schema schema)
    {
        return expression.CloneWithReplacement(nested => baseExpressions.Contains(nested) 
            ? ExpressionAsColumn(nested, schema) 
            : null);

        static ILogicalExpression ExpressionAsColumn(ILogicalExpression expression, Schema schema)
        {
            if (expression is not Column c)
            {
                return new Column(expression.CreateName());
            }

            var field = schema.GetFieldFromColumn(c);
            return field!.QualifiedColumn();
        }
    }
    /// <summary>
    /// Converts an AST SQL expression into a logical expression
    /// </summary>
    /// <param name="expression">Expression to convert</param>
    /// <param name="schema">Schema containing column definitions</param>
    /// <param name="context">Planner context</param>
    /// <returns>Logical expression instance</returns>
    internal static ILogicalExpression SqlExpressionToLogicalExpression(
        Expression? expression, Schema schema, PlannerContext context)
    {
        if (expression is BinaryOp b)
        {
            return ParseSqlBinaryOp(b.Left, b.Op, b.Right, schema, context);
        }

        return SqlExpressionToLogicalInternal(expression, schema, context);
    }
    /// <summary>
    /// Converts literal values, identifiers, compound identifiers,
    /// and functions into logical expressions
    /// </summary>
    /// <param name="expression">Expression to convert</param>
    /// <param name="schema">Schema containing column definitions</param>
    /// <param name="context">Planner context</param>
    /// <returns>Logical expression instance</returns>
    /// <exception cref="NotImplementedException"></exception>
    internal static ILogicalExpression SqlExpressionToLogicalInternal(Expression? expression, Schema schema, PlannerContext context)
    {
        return expression switch
        {
            LiteralValue v => v.ParseValue(),
            Identifier ident => ident.SqlIdentifierToExpression(schema, context),
            Function fn => SqlFunctionToExpression(fn, schema, context),
            CompoundIdentifier ci => SqlCompoundIdentToExpression(ci, schema),

            _ => throw new NotImplementedException()
        };
    }
    /// <summary>
    /// Converts a SQL Identifier into an unqualified column
    /// </summary>
    /// <param name="ident">SQL identifier</param>
    /// <param name="schema">Schema containing column definitions</param>
    /// <param name="context">Planner context</param>
    /// <returns>Column instance</returns>
    internal static Column SqlIdentifierToExpression(this Identifier ident, Schema schema, PlannerContext context)
    {
        // Found a match without a qualified name, this is a inner table column
        var field = schema.GetField(ident.Ident.Value);
        if (field != null)
        {
            return new Column(schema.GetField(ident.Ident.Value)!.Name);
        }
        
        //if (context.OuterQuerySchema != null)
        //{
        //    //todo df/sql/source/expr/identifier.rs
        //}

        return new Column(ident.Ident.Value);
    }
    /// <summary>
    /// Parses a SQL Binary operator into a set of logical expressions
    /// in the form of a logical binary expression
    /// </summary>
    /// <param name="left">Operation left expression</param>
    /// <param name="op">Operator</param>
    /// <param name="right">Operation right expression</param>
    /// <param name="schema">Schema containing column definitions</param>
    /// <param name="context">Planner context</param>
    /// <returns>Binary expression</returns>
    internal static Binary ParseSqlBinaryOp(
        Expression left, 
        BinaryOperator op, 
        Expression right,
        Schema schema,
        PlannerContext context)
    {
        return new Binary(
            SqlExpressionToLogicalExpression(left, schema, context), 
            op, 
            SqlExpressionToLogicalExpression(right, schema, context));
    }
    /// <summary>
    /// Parses SQL literal values into logical expression types
    /// </summary>
    /// <param name="literalValue">SQL literal value</param>
    /// <returns>Logical expression instance</returns>
    /// <exception cref="NotImplementedException"></exception>
    internal static ILogicalExpression ParseValue(this LiteralValue literalValue)
    {
        switch (literalValue.Value)
        {
            //TODO case Value.Null: literal scalar value

            case Value.Number n:
                return n.ParseSqlNumber();

            case Value.SingleQuotedString sq:
                return new Literal(new StringScalar(sq.Value));

            case Value.Boolean b:
                return new Literal(new BooleanScalar(b.Value));

            default:
                throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Converts a SQL function to a logical expression
    /// </summary>
    /// <param name="function">SQL function to convert</param>
    /// <param name="schema">Schema containing column definitions</param>
    /// <param name="context">Planner context</param>
    /// <returns>Logical expression instance</returns>
    /// <exception cref="InvalidOperationException"></exception>
    internal static ILogicalExpression SqlFunctionToExpression(Function function, Schema schema, PlannerContext context)
    {
        // scalar functions

        // aggregate functions
        var name = function.Name;

        var aggregateType = AggregateFunction.GetFunctionType(name);
        if (aggregateType.HasValue)
        {
            var distinct = function.Distinct;

            var (aggregateFunction, expressionArgs) = 
                AggregateFunctionToExpression(aggregateType.Value, function.Args, schema, context);

            return new AggregateFunction(aggregateFunction, expressionArgs, distinct);
        }

        throw new InvalidOperationException("Invalid function");
    }
    /// <summary>
    /// Parses a SQL numeric value into a literal value
    /// containing a type-specific scalar value
    /// </summary>
    /// <param name="number">SQL number to convert </param>
    /// <returns>Logical expression instance</returns>
    internal static ILogicalExpression ParseSqlNumber(this Value.Number number)
    {
        if (long.TryParse(number.Value, out var parsedInt))
        {
            return new Literal(new IntegerScalar(parsedInt));
        }

        return double.TryParse(number.Value, out var parsedDouble)
            ? new Literal(new DoubleScalar(parsedDouble))
            : new Literal(new StringScalar(number.Value));
    }
    /// <summary>
    /// Converts a compound identifier into a logical expression
    /// </summary>
    /// <param name="ident">Identifier to convert</param>
    /// <param name="schema">Schema containing column definitions</param>
    /// <returns>Logical expression instance</returns>
    /// <exception cref="InvalidOperationException"></exception>
    /// <exception cref="NotImplementedException"></exception>
    internal static ILogicalExpression SqlCompoundIdentToExpression(CompoundIdentifier ident, Schema schema)
    {
        if (ident.Idents.Count > 2)
        {
            throw new InvalidOperationException("Not a valid compound identifier");
        }

        var idents = ident.Idents.Select(i => i.Value).ToList();
        var terms = idents.GenerateSearchTerms().ToList();

        var result = terms.Select(term => new
            {
                Term = term,
                Field = schema.GetFieldFromColumn(new Column(term.ColumnName, term.Table))
            })
            .Where(term => term.Field != null)
            .Select(term => (term.Field, term.Term.NestedNames))
            .FirstOrDefault();


        if (result.Field != null && result.NestedNames.Length == 0)
        {
            return result.Field.QualifiedColumn();
        }

        throw new NotImplementedException("SqlCompoundIdentToExpression not implemented for identifier");
        // todo case field & nested is not empty
        // case null
    }

    internal static List<ILogicalExpression> FindNestedExpressions(List<ILogicalExpression> expressions, Func<ILogicalExpression, bool> predicate)
    {
        return expressions
            .SelectMany(e => FindNestedExpression(e, predicate))
            .Aggregate(new List<ILogicalExpression>(), (list, value) =>
            {
                if (!list.Contains(value)) { list.Add(value); }

                return list;
            })
            .ToList();
    }

    internal static IEnumerable<ILogicalExpression> FindNestedExpression(ILogicalExpression expression, Func<ILogicalExpression, bool> predicate)
    {
        var expressions = new List<ILogicalExpression>();
        expression.Apply(e =>
        {
            if (!predicate((ILogicalExpression)e))
            {
                return VisitRecursion.Continue;
            }

            if (!expressions.Contains(e))
            {
                expressions.Add((ILogicalExpression)e);
            }

            return VisitRecursion.Skip;

        });

        return expressions;
    }
    /// <summary>
    /// Converts a schema into a nested list containing the single schema
    /// </summary>
    /// <param name="schema">Schema to insert into the hierarchy</param>
    /// <returns>List of schema lists</returns>
    internal static List<List<Schema>> AsNested(this Schema schema)
    {
        return new List<List<Schema>> { new() { schema } };
    }
    /// <summary>
    /// Converts a hash set into a nested list containing the single hash set
    /// </summary>
    /// <param name="usingColumns">Hash set to insert into the hierarchy</param>
    /// <returns>List of hash sets with column values</returns>
    internal static List<HashSet<Column>> AsNested(this HashSet<Column> usingColumns)
    {
        return new List<HashSet<Column>> { usingColumns };
    }
    /// <summary>
    /// Merges one schema into another by selecting distinct fields from the second schema into the first
    /// </summary>
    /// <param name="self">Target schema to merge fields into</param>
    /// <param name="fromSchema">Schema containing fields to merge</param>
    internal static void MergeSchemas(this Schema self, Schema fromSchema)
    {
        if (!fromSchema.Fields.Any())
        {
            return;
        }

        foreach (var field in fromSchema.Fields)
        {
            var duplicate = self.Fields.FirstOrDefault(f =>  f.Name == field.Name) != null;

            if (!duplicate)
            {
                self.Fields.Add(field);
            }
        }
    }
    /// <summary>
    /// Used for normalizing columns as the fallback schemas to
    /// a plan's main schema
    /// </summary>
    /// <param name="plan">Plan to normalize</param>
    /// <returns>List of schemas</returns>
    internal static List<Schema> FallbackNormalizeSchemas(this ILogicalPlan plan)
    {
        return plan switch
        {
            Projection
                or Aggregate
                or Join => plan.GetInputs().Select(input => input.Schema).ToList(),

            // TODO or Cross join

            _ => new List<Schema>()
        };
    }
    /// <summary>
    /// Generates a list of possible search terms from a list
    /// of identifiers
    ///
    /// Length = 2
    /// (table.column)
    /// (column).nested
    ///
    /// Length = 3:
    /// 1. (schema.table.column)
    /// 2. (table.column).nested
    /// 3. (column).nested1.nested2
    ///
    /// Length = 4:
    /// 1. (catalog.schema.table.column)
    /// 2. (schema.table.column).nested1
    /// 3. (table.column).nested1.nested2
    /// 4. (column).nested1.nested2.nested3
    /// </summary>
    /// <param name="idents">Identifier list used to build search values</param>
    /// <returns>List of table references and column names</returns>
    internal static List<(TableReference? Table, string ColumnName, string[] NestedNames)>
        GenerateSearchTerms(this IReadOnlyCollection<string> idents)
    {
        var ids = idents.ToArray();
        // at most 4 identifiers to form a column to search with
        // 1 for the column name
        // 0 - 3 for the table reference
        var bound = Math.Min(idents.Count, 4);

        return Enumerable.Range(0, bound).Reverse().Select(i =>
        {
            var nestedNamesIndex = i + 1;
            var qualifierWithColumn = ids[..nestedNamesIndex];
            var (relation, columnName) = FromIdentifier(qualifierWithColumn);

            return (relation, columnName, ids[nestedNamesIndex..]);
        }).ToList();
    }

    private static (TableReference?, string) FromIdentifier(IReadOnlyList<string> idents)
    {
        return idents.Count switch
        {
            1 => (null, idents[0]),
            2 => (new TableReference(idents[0]), idents[1]),

            _ => throw new InvalidOperationException("Incorrect number of identifiers")
        };
    }

    #endregion

    #region Table Plan

    /// <summary>
    /// Gets the root logical plan.  The plan root will scan the data
    /// source for the query's projected values. The plan is empty in
    /// the case there is no from clause
    ///  e.g. `select 123`
    /// </summary>
    /// <param name="tables">Data sources used to look up the table being scanned</param>
    /// <param name="context">Planner context</param>
    /// <returns>ILogicalPlan instance as the plan root</returns>
    /// <exception cref="InvalidOperationException">Thrown for unsupported from clauses</exception>
    internal static ILogicalPlan PlanTableWithJoins(this IReadOnlyCollection<TableWithJoins>? tables, PlannerContext context)
    {
        if (tables == null || tables.Count == 0)
        {
            return new EmptyRelation(true);
        }

        var table = tables.First();
        var left = table.Relation!.CreateRelation(context);

        if (table.Joins == null || !table.Joins.Any())
        {
            return left;
        }

        return table.Joins.Aggregate(left, (current, join) => current.ParseRelationJoin(join, context));
    }

    internal static ILogicalPlan CreateRelation(this TableFactor tableFactor, PlannerContext context)
    {
        if (tableFactor is not TableFactor.Table relation)
        {
            throw new InvalidOperationException();
        }

        // Get the table name used to query the data source
        // var name = relation.Alias != null ? relation.Alias.Name : relation.Name.Values[0];
        var name = relation.Name.Values[0];
        var tableRef = context.TableReferences.Find(t => t.Name == name);

        // The root operation will scan the table for the projected values
        var table = context.DataSources[name];
        var qualifiedFields = table.Schema!.Fields.Select(f => new QualifiedField(f.Name, f.DataType, tableRef)).ToList();
        var schema = new Schema(qualifiedFields);

        var plan = (ILogicalPlan)new TableScan(name, schema, table);

        return tableRef?.Alias == null 
            ? plan 
            : SubqueryAlias.TryNew(plan, tableRef.Alias);
    }

    internal static ILogicalPlan ParseRelationJoin(this ILogicalPlan left, SqlParser.Ast.Join join, PlannerContext context)
    {
        var right = join.Relation!.CreateRelation(context);

        switch (join.JoinOperator)
        {
            case JoinOperator.LeftOuter l:
                return ParseJoin(left, right, l.JoinConstraint, JoinType.Left, context);

            case JoinOperator.RightOuter r:
                return ParseJoin(left, right, r.JoinConstraint, JoinType.Right, context);

            case JoinOperator.Inner i:
                return ParseJoin(left, right, i.JoinConstraint, JoinType.Inner, context);

            case JoinOperator.LeftSemi ls:
                return ParseJoin(left, right, ls.JoinConstraint, JoinType.LeftSemi, context);

            case JoinOperator.RightSemi rs:
                return ParseJoin(left, right, rs.JoinConstraint, JoinType.RightSemi, context);

            case JoinOperator.LeftAnti la:
                return ParseJoin(left, right, la.JoinConstraint, JoinType.LeftAnti, context);

            case JoinOperator.RightAnti ra:
                return ParseJoin(left, right, ra.JoinConstraint, JoinType.RightAnti, context);

            case JoinOperator.FullOuter f:
                return ParseJoin(left, right, f.JoinConstraint, JoinType.Full, context);

            //case JoinOperator.CrossJoin:
            //    break;
            default:
                throw new NotImplementedException("ParseRelationJoin Join type not implemented yet");
        }
    }

    internal static ILogicalPlan ParseJoin(
        ILogicalPlan left,
        ILogicalPlan right,
        JoinConstraint constraint,
        JoinType joinType,
        PlannerContext context)
    {
        if (constraint is not JoinConstraint.On on)
        {
            throw new NotImplementedException("ParseJoin: Join constraint not implemented");
        }

        var joinSchema = left.Schema.Join(right.Schema);
        var expression = on.Expression.SqlToExpression(joinSchema, context);
        
        return Join.TryNew(left, right, joinType, (new List<Column>(), new List<Column>()), constraint, expression);
    }

    internal static List<TableReference> CreateTableRelations(this IElement select)
    {
        var relationVisitor = new RelationVisitor();
        select.Visit(relationVisitor);
        return relationVisitor.TableReferences;
    }
    #endregion

    #region Select Plan

    /// <summary>
    /// Builds a logical plan from a query filter
    /// </summary>
    /// <param name="selection">Filter expression</param>
    /// <param name="plan">Input plan</param>
    /// <param name="context">Planner context</param>
    /// <returns>ILogicalPlan instance to filter the input plan</returns>
    internal static ILogicalPlan PlanFromSelection(this Expression? selection, ILogicalPlan plan, PlannerContext context)
    {
        if (selection == null)
        {
            return plan;
        }

        var filterExpression = selection.SqlToExpression(plan.Schema, context);
        var usingColumns = new HashSet<Column>();
        filterExpression.ExpressionToColumns(usingColumns);

        filterExpression = NormalizeColumnWithSchemas(filterExpression, plan.Schema.AsNested(), usingColumns.AsNested());
        
        return new Filter(plan, filterExpression);
    }

    /// <summary>
    /// Create a projection from a `SELECT` statement
    /// </summary>
    /// <param name="projection"></param>
    /// <param name="plan"></param>
    /// <param name="emptyFrom"></param>
    /// <param name="context">Planner context</param>
    /// <returns>List of parsed select expressions</returns>
    internal static List<ILogicalExpression> PrepareSelectExpressions(
        this IEnumerable<SelectItem> projection,
        ILogicalPlan plan, 
        bool emptyFrom,
        PlannerContext context)
    {
        return projection.Select(SelectToRelationalExpression).SelectMany(_ => _).ToList();

        List<ILogicalExpression> SelectToRelationalExpression(SelectItem sql)
        {
            switch (sql)
            {
                case SelectItem.UnnamedExpression u:
                    {
                        var expr = u.Expression.SqlToExpression(plan.Schema, context);
                        var column = expr.NormalizeColumnWithSchemas(plan.Schema.AsNested(), plan.UsingColumns);
                        return new List<ILogicalExpression> { column };
                    }
                case SelectItem.ExpressionWithAlias e:
                    {
                        var select = e.Expression.SqlToExpression(plan.Schema, context);
                        var column = select.NormalizeColumnWithSchemas(plan.Schema.AsNested(), plan.UsingColumns);
                        return new List<ILogicalExpression> { new Alias(column, e.Alias) };
                    }
                case SelectItem.Wildcard:
                    if (emptyFrom)
                    {
                        throw new InvalidOperationException("SELECT * with no table is not valid");
                    }

                    //TODO expand wildcard select.rs line 320
                    return plan.Schema.ExpandWildcard();

                default:
                    throw new InvalidOperationException("Invalid select expression");
            }
        }
    }
    /// <summary>
    /// See the Column-specific documentation
    /// </summary>
    /// <param name="expression">Expression to normalize</param>
    /// <param name="schemas">Schema hierarchy to search for column names</param>
    /// <param name="usingColumns">Column groups containing using columns</param>
    /// <returns>Logical expression with a normalized name</returns>
    internal static ILogicalExpression NormalizeColumnWithSchemas(
        this ILogicalExpression expression,
        List<List<Schema>> schemas,
        List<HashSet<Column>> usingColumns)
    {
        return expression.Transform(expression, e =>
        {
            if (e is Column c)
            {
                return c.NormalizeColumnWithSchemas(schemas, usingColumns);
            }

            return e;
        });
    }
    /// <summary>
    /// Qualify a column if it has not yet been qualified.  For unqualified
    /// columns, schemas are searched for the relevant column.  Due to
    /// SQL syntax behavior, columns can be ambiguous.  This will check
    /// for a single schema match when the column is unmatched (not assigned
    /// to a given table source).
    ///
    /// For example, the following SQL query
    /// <code>SELECT name table1 JOIN table2 USING (name)</code>
    /// 
    /// table1.name and table2.name will match unqualified column 'name'
    /// hence the list of HashSet column lists that maps columns
    /// together to help check ambiguity.  Schemas are also nested in a
    /// list of lists so they can be checked at various logical depths
    /// </summary>
    /// <param name="column">Column to normalize</param>
    /// <param name="schemas">Schema hierarchy to search for column names</param>
    /// <param name="usingColumns">Column groups containing using columns</param>
    /// <returns>Logical expression with a normalized name</returns>
    /// <exception cref="NotImplementedException"></exception>
    /// <exception cref="InvalidOperationException"></exception>
    internal static Column NormalizeColumnWithSchemas(
        this Column column,
        List<List<Schema>> schemas,
        List<HashSet<Column>> usingColumns) //TODO: unused argument
    {
        if (column.Relation != null)
        {
            return column;
        }

        foreach (var schemaLevel in schemas)
        {
            var fields = schemaLevel.SelectMany(s => s.FieldsWithUnqualifiedName(column.Name)).ToList();

            switch (fields.Count)
            {
                case 0: continue;
                case 1: return fields[0].QualifiedColumn();
                default:
                    throw new NotImplementedException("Needs to be implemented");

            }
        }

        throw new InvalidOperationException("field not found");
    }
    /// <summary>
    /// Convert a Wildcard statement into a list of columns related to the
    /// underlying schema data source
    /// </summary>
    /// <param name="schema">Schema containing all fields to be expanded</param>
    /// <returns>List of expressions expanded from the wildcard</returns>
    internal static List<ILogicalExpression> ExpandWildcard(this Schema schema)
    {
        // todo using columns for join
        return schema.Fields.Select(f => (ILogicalExpression)f.QualifiedColumn()).ToList();
    }

    #endregion

    #region Aggregate Plan
    /// <summary>
    /// Crete an aggregate plan from a list of SELECT, HAVING, GROUP BY, and Aggregate expressions
    /// </summary>
    /// <param name="plan">Logical plan to wrap</param>
    /// <param name="selectExpressions">Select expression list</param>
    /// <param name="havingExpression">Query HAVING expression list</param>
    /// <param name="groupByExpressions">Query GROUP BY expression list</param>
    /// <param name="aggregateExpressions">Aggregate function expression list</param>
    /// <returns></returns>
    internal static (ILogicalPlan, List<ILogicalExpression>, ILogicalExpression?) CreateAggregatePlan(
        this ILogicalPlan plan,
        List<ILogicalExpression> selectExpressions,
        ILogicalExpression? havingExpression,
        List<ILogicalExpression> groupByExpressions,
        List<ILogicalExpression> aggregateExpressions)
    {
        var groupingSets = groupByExpressions;
        var allExpressions = groupingSets.Concat(aggregateExpressions).ToList();

        var fields = allExpressions.ExpressionListToFields(plan);
        var schema = new Schema(fields);
        var aggregatePlan = new Aggregate(plan, groupByExpressions, aggregateExpressions, schema);

        var aggregateProjectionExpressions = groupByExpressions
            .ToList()
            .Concat(aggregateExpressions)
            .Select(e => ResolveColumns(e, plan.Schema))
            .ToList();

        var selectExpressionsPostAggregate = selectExpressions.Select(e => e.RebaseExpression(aggregateProjectionExpressions, plan.Schema)).ToList();

        // rewrite having columns to use columns in by the aggregation
        ILogicalExpression? havingPostAggregation = null;

        if (havingExpression != null)
        {
            havingPostAggregation = havingExpression.RebaseExpression(aggregateProjectionExpressions, plan.Schema);
        }

        return (aggregatePlan, selectExpressionsPostAggregate, havingPostAggregation);
    }

    internal static (AggregateFunctionType, List<ILogicalExpression>) AggregateFunctionToExpression(
        AggregateFunctionType functionType,
        IReadOnlyCollection<FunctionArg>? args,
        Schema schema, 
        PlannerContext context)
    {
        List<ILogicalExpression> arguments;

        if (functionType == AggregateFunctionType.Count)
        {
            functionType = AggregateFunctionType.Count;
            arguments = FunctionArgsToExpression();
        }
        else
        {
            arguments = FunctionArgsToExpression();
        }

        return (functionType, arguments);

        List<ILogicalExpression> FunctionArgsToExpression()
        {
            return args == null
                ? new List<ILogicalExpression>()
                : args.Select(SqlFnArgToLogicalExpression).ToList();

            ILogicalExpression SqlFnArgToLogicalExpression(FunctionArg functionArg)
            {
                return functionArg switch
                {
                    FunctionArg.Named { Arg: FunctionArgExpression.Wildcard } => new Wildcard(),
                    FunctionArg.Named { Arg: FunctionArgExpression.FunctionExpression a }
                        => SqlExpressionToLogicalExpression(a.Expression, schema, context),
                    FunctionArg.Unnamed { FunctionArgExpression: FunctionArgExpression.FunctionExpression fe }
                        => SqlExpressionToLogicalExpression(fe.Expression, schema, context),

                    _ => throw new InvalidOleVariantTypeException($"Unsupported qualified wildcard argument: {functionArg.ToSql()}")
                };
            }
        }
    }

    internal static List<ILogicalExpression> FindGroupByExpressions(
        this IReadOnlyCollection<Expression>? selectGroupBy,
        List<ILogicalExpression> selectExpressions,
        Schema combinedSchema,
        ILogicalPlan plan,
        Dictionary<string, ILogicalExpression> aliasMap,
        PlannerContext context)
    {
        if (selectGroupBy == null)
        {
            return new List<ILogicalExpression>();
        }

        return selectGroupBy.Select(expr =>
        {
            var groupByExpr = SqlExpressionToLogicalExpression(expr, combinedSchema, context);

            foreach (var field in plan.Schema.Fields)
            {
                aliasMap.Remove(field.Name);
            }

            groupByExpr = groupByExpr.ResolveAliasToExpressions(aliasMap);
            groupByExpr = groupByExpr.ResolvePositionsToExpressions(selectExpressions) ?? groupByExpr;
            groupByExpr = groupByExpr.NormalizeColumn(plan);

            return groupByExpr;
        }).ToList();
    }

    internal static List<ILogicalExpression> NormalizeColumn(this IEnumerable<ILogicalExpression> expressions, ILogicalPlan plan)
    {
        return expressions.Select(e => e.NormalizeColumn(plan)).ToList();
    }

    internal static ILogicalExpression NormalizeColumn(this ILogicalExpression expression, ILogicalPlan plan)
    {
        return expression.Transform(expression, e =>
        {
            if (e is Column c)
            {
                return c.Normalize(plan);
            }

            return e;
        });
    }

    internal static ILogicalExpression Normalize(this Column column, ILogicalPlan plan)
    {
        var schema = plan.Schema;
        var fallback = plan.FallbackNormalizeSchemas();
        var usingColumns = plan.UsingColumns;
        var schemas = new List<List<Schema>> { new() { schema }, fallback };
        var normalized = NormalizeColumnWithSchemas(column, schemas, usingColumns);

        return normalized;
    }

    internal static ILogicalExpression? MapHaving(
        this Expression? having, 
        Schema schema,
        Dictionary<string, ILogicalExpression> aliasMap,
        PlannerContext context)
    {
        var havingExpression = having == null ? null : SqlExpressionToLogicalExpression(having, schema, context);

        // This step swaps aliases in the HAVING clause for the
        // underlying column name.  This is how the planner supports
        // queries with HAVING expressions that refer to aliased columns.
        //
        //   SELECT c1, MAX(c2) AS abc FROM tbl GROUP BY c1 HAVING abc > 10;
        //
        // is rewritten
        //
        //   SELECT c1, MAX(c2) AS abc FROM tbl GROUP BY c1 HAVING MAX(c2) > 10;
        return havingExpression?.ResolveAliasToExpressions(aliasMap);
    }

    internal static ILogicalExpression ResolveAliasToExpressions(this ILogicalExpression expression,
        IReadOnlyDictionary<string, ILogicalExpression> aliasMap)
    {
        return expression.CloneWithReplacement(e =>
        {
            if (e is Column c && aliasMap.ContainsKey(c.Name))
            {
                return aliasMap[c.Name];
            }

            return null;
        });
    }

    private static ILogicalExpression? ResolvePositionsToExpressions(this ILogicalExpression expression,
        IReadOnlyList<ILogicalExpression> selectExpressions)
    {
        if (expression is not Literal { Value: IntegerScalar i })
        {
            return null;
        }

        var position = (int)i.Value - 1;
        var expr = selectExpressions[position];

        if (expr is Alias a)
        {
            return a.Expression;
        }

        return expr;
    }

    internal static List<ILogicalExpression> FindAggregateExpressions(this List<ILogicalExpression> expressions)
    {
        return FindNestedExpressions(expressions, nested => nested is AggregateFunction);
    }
    #endregion

    #region Projection Plan
    internal static ILogicalPlan PlanProjection(this ILogicalPlan plan, List<ILogicalExpression> expressions)
    {
        var projectedExpressions = new List<ILogicalExpression>();

        foreach (var expr in expressions)
        {
            if (expr is Wildcard /*or QualifiedWildcard*/)
            {
                throw new NotImplementedException("Need to implement wildcard");
            }
            //else 
            //{
            var normalized = NormalizeColumnWithSchemas(expr, plan.Schema.AsNested(), new List<HashSet<Column>>());
            projectedExpressions.Add(ToColumnExpression(normalized, plan.Schema));
            //}
        }

        var fields = projectedExpressions.ExpressionListToFields(plan);
        return new Projection(plan, expressions, new Schema(fields));

        static ILogicalExpression ToColumnExpression(ILogicalExpression expression, Schema schema)
        {
            switch (expression)
            {
                case Column:
                    return expression;

                case Alias alias:
                    return alias with { Expression = ToColumnExpression(alias.Expression, schema) };

                // case Cast
                // case TryCast
                // case ScalarSubQuery

                default:
                    var name = expression.CreateName();
                    var field = schema.GetField(name);
                    return field?.QualifiedColumn() ?? expression;
            }
        }
    }
    
    internal static ILogicalPlan AddMissingColumns(this ILogicalPlan plan, HashSet<Column> missingColumns, bool isDistinct)
    {
        if (plan is Projection projection)
        {
            if (missingColumns.All(projection.Plan.Schema.HasColumn))
            {
                var missingExpressions = missingColumns.Select(c =>
                    NormalizeColumn(new Column(c.Name, c.Relation), plan)
                ).ToList();

                // Do not let duplicate columns to be added, some of the missing columns
                // may be already present but without the new projected alias.
                missingExpressions = missingExpressions.Where(c =>!projection.Expression.Contains(c)).ToList();

                if (isDistinct)
                {
                    //distinct check
                }

                projection.Expression.AddRange(missingExpressions);
                return PlanProjection(projection.Plan, projection.Expression);
            }
        }

        var distinct = isDistinct || plan is Distinct;

        var newInputs = plan.GetInputs()
            .Select(p => p.AddMissingColumns(missingColumns, distinct))
            .ToList();

        return plan.FromPlan(plan.GetExpressions(), newInputs);
    }
    #endregion

    #region Order By Plan
    internal static ILogicalPlan OrderBy(
        this ILogicalPlan plan, 
        Sequence<OrderByExpression>? orderByExpressions, 
        PlannerContext context)
    {
        if (orderByExpressions == null || !orderByExpressions.Any())
        {
            return plan;
        }

        var orderByRelation = orderByExpressions.Select(e => e.OrderByToSortExpression(plan.Schema, context)).ToList();

        return Sort.TryNew(plan, orderByRelation);
    }

    private static ILogicalExpression OrderByToSortExpression(
        this OrderByExpression orderByExpression,
        Schema schema,
        PlannerContext context)
    {
        ILogicalExpression orderExpression;
        if (orderByExpression.Expression is LiteralValue{Value:Value.Number n})
        {
            var fieldIndex = int.Parse(n.Value);
            var field = schema.Fields[fieldIndex - 1];
            orderExpression = field.QualifiedColumn();
        }
        else
        {
            orderExpression = SqlExpressionToLogicalExpression(orderByExpression.Expression, schema, context);
        }

        return new OrderBy(orderExpression, orderByExpression.Asc ?? true);
    }

    #endregion

    #region Limit Plan
    internal static ILogicalPlan Limit(this ILogicalPlan plan, Offset? skip, Expression? fetch)
    {
        if (skip == null && fetch == null)
        {
            return plan;
        }

        var skipCount = 0;
        var fetchCount = int.MaxValue;

        if (skip != null)
        {
            if (skip.Value is LiteralValue slv)
            {
                if (slv.Value is Value.Number skipNumber)
                {
                    _ = int.TryParse(skipNumber.Value, out skipCount);
                }
                else
                {
                    throw new InvalidOperationException("Invalid offset");
                }
            }
            else
            {
                throw new InvalidOperationException("Invalid offset");
            }
        }

        if (fetch != null)
        {
            if (fetch is LiteralValue flv)
            {
                if (flv.Value is Value.Number fetchNumber)
                {
                    _ = int.TryParse(fetchNumber.Value, out fetchCount);
                }
                else
                {
                    throw new InvalidOperationException("Invalid offset");
                }
            }
            else
            {
                throw new InvalidOperationException("Invalid offset");
            }
        }

        return new Limit(plan, skipCount, fetchCount);
    }
    #endregion

    #region Join Plan
    internal static Schema BuildJoinSchema(Schema left, Schema right, JoinType joinType)
    {
        var leftFields = left.Fields;
        var rightFields = right.Fields;

        // No default branch; all options accounted for.
#pragma warning disable CS8524
        var fields = joinType switch
#pragma warning restore CS8524
        {
            JoinType.Inner
                or JoinType.Full
                or JoinType.Right => leftFields.Concat(rightFields).ToList(),

            JoinType.Left => GetLeftJoinFields(),

            JoinType.LeftSemi or JoinType.LeftAnti => leftFields,
            JoinType.RightSemi or JoinType.RightAnti => rightFields
        };

        List<QualifiedField> GetLeftJoinFields()
        {
            var rightFieldsNullable = rightFields
                .Select(f => f.Qualifier != null
                    ? new QualifiedField(f.Name, f.DataType, f.Qualifier)
                    : new QualifiedField(f.Name, f.DataType)).ToList();

            return leftFields.ToList().Concat(rightFieldsNullable).ToList();
        }

        return new Schema(fields);
    }
    #endregion

    #region Physical Expression
    internal static IPhysicalExpression CreatePhysicalExpression(this ILogicalExpression expression, Schema inputDfSchema, Schema inputSchema)
    {
        while (true)
        {
            switch (expression)
            {
                case Column c:
                    var index = inputDfSchema.IndexOfColumn(c);
                    return new Physical.Expressions.Column(c.Name, index!.Value);

                case Literal l:
                    return new Physical.Expressions.Literal(l.Value);

                case Alias a:
                    expression = a.Expression;
                    continue;

                case Binary b:
                    {
                        var left = b.Left.CreatePhysicalExpression(inputDfSchema, inputSchema);
                        var right = b.Right.CreatePhysicalExpression(inputDfSchema, inputSchema);

                        return new Physical.Expressions.Binary(left, b.Op, right);
                    }

                default:
                    throw new NotImplementedException($"Expression type {expression.GetType().Name} is not yet supported.");
            }
        }
    }

    internal static string GetPhysicalName(this ILogicalExpression expression)
    {
        return expression switch
        {
            Column c => c.Name,
            Binary b => $"{b.Left.GetPhysicalName()} {b.Op} {b.Right.GetPhysicalName()}",
            Alias a => a.Name,
            AggregateFunction fn => fn.CreateFunctionPhysicalName(fn.Distinct, fn.Args),
            _ => throw new NotImplementedException()
        };
    }

    internal static string CreateFunctionPhysicalName(this AggregateFunction fn, bool distinct, List<ILogicalExpression> args)
    {
        var names = args.Select(e => CreatePhysicalName(e, false)).ToList();

        var distinctText = distinct ? "DISTINCT " : "";

        return $"{fn.FunctionType}({distinctText}{string.Join(",", names)})";
    }

    //TODO isFirst variable unused
    internal static string CreatePhysicalName(this ILogicalExpression expression, bool isFirst)
    {
        switch (expression)
        {
            case Column c:
                return c.Name;//todo is first?name:flatname

            case Binary b:
                return $"{CreatePhysicalName(b.Left, false)} {b.Op} {CreatePhysicalName(b.Left, false)}";

            case AggregateFunction fn:
                return fn.CreateFunctionPhysicalName(fn.Distinct, fn.Args);

            default:
                throw new NotImplementedException();
        }
    }

    #endregion
}