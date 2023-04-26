﻿using CsvRx.Logical.Expressions;
using CsvRx.Logical.Functions;
using CsvRx.Logical;
using CsvRx.Data;
using CsvRx.Logical.Plans;
using SqlParser.Ast;
using System.Runtime.InteropServices;
using CsvRx.Physical.Expressions;

namespace CsvRx;

internal static class Extensions
{
    internal static string CreateName(this ILogicalExpression expr)
    {
        return expr switch
        {
            //Alias
            Column c => c.Name, //{}.{}
            BinaryExpr b => $"{CreateName(b.Left)} {b.Op} {CreateName(b.Right)}",
            AggregateFunction fn => GetFunctionName(fn, false, fn.Args),
            LiteralExpression l => l.Value.RawValue.ToString(),
            //Like
            // Case
            // cast
            // not
            // is null
            // isnotnull
            Wildcard => "*",
            _ => throw new NotImplementedException("need to implement")
        };

        //string GetBinaryName(BinaryExpr binary)
        //{
        //    var left = CreateName(binary.Left);
        //    var right = CreateName(binary.Right);
        //    return $"{left} {binary.Op} {right}";
        //}

        string GetFunctionName(AggregateFunction fn, bool distinct, List<ILogicalExpression> args)
        {
            var names = args.Select(CreateName).ToList();
            var distinctName = distinct ? "DISTINCT " : string.Empty;
            var functionName = fn.FunctionType.ToString().ToUpperInvariant();
            return $"{functionName}({distinctName}{string.Join(",", names)})";
        }
    }

    internal static void ExprListToColumns(List<ILogicalExpression> expressions, HashSet<Column> accumulator)
    {
        foreach (var expr in expressions)
        {
            ExprToColumns(expr, accumulator);
        }
    }

    internal static void ExprToColumns(ILogicalExpression expression, HashSet<Column> accumulator)
    {
        InspectExprPre(expression, Inspect);

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

    internal static void InspectExprPre(ILogicalExpression expression, Action<ILogicalExpression> action)
    {
        expression.Apply(expr =>
        {
            try
            {
                action((ILogicalExpression)expr);
                return VisitRecursion.Continue;
            }
            catch
            {
                return VisitRecursion.Stop;
            }
        });
    }

    internal static List<Field> ExprListToFields(IEnumerable<ILogicalExpression> expressions, ILogicalPlan plan)
    {
        return expressions.Select(e => ToField(e, plan.Schema)).ToList();
    }

    internal static Field ToField(ILogicalExpression expression, Schema schema)
    {
        if (expression is not Column c)
        {
            return new Field(expression.CreateName(), GetDataType(expression, schema));
        }

        var field = schema.GetField(c.Name)!;
        return field;

    }

    internal static ColumnDataType GetDataType(ILogicalExpression expression, Schema schema)
    {
        return expression switch
        {
            Column c => schema.GetField(c.Name)!.DataType,
            AggregateFunction fn => GetAggregateDataType(fn),
            _ => throw new NotImplementedException(),
        };

        ColumnDataType GetAggregateDataType(AggregateFunction function)
        {
            var dataTypes = function.Args.Select(e => GetDataType(e, schema)).ToList();

            return function.FunctionType switch
            {
                AggregateFunctionType.Max => CoercedTypes(function, dataTypes),
                _ => throw new NotImplementedException("need to implement"),
            };
        }

        ColumnDataType CoercedTypes(AggregateFunction function, IReadOnlyList<ColumnDataType> inputTypes)
        {
            return function.FunctionType switch
            {
                AggregateFunctionType.Min or AggregateFunctionType.Max => GetMinMaxType(),//inputTypes
                _ => throw new NotImplementedException("need to implement"),
            };

            ColumnDataType GetMinMaxType()//IReadOnlyList<ColumnDataType> inputTypes
            {
                if (inputTypes.Count != 1)
                {
                    throw new InvalidOperationException();
                }

                return inputTypes[0];
            }
        }
    }


    #region Table Plan
    /// <summary>
    /// Gets the root logical plan.  The plan root will scan the data
    /// source for the query's projected values. The plan is empty in
    /// the case there is no from clause
    ///  e.g. `select 123`
    /// </summary>
    /// <param name="tables">Data sources used to look up the table being scanned</param>
    /// <param name="dataSources">Query from clause.  This should contain zero or one statements.</param>
    /// <returns>ILogicalPlan instance as the plan root</returns>
    /// <exception cref="InvalidOperationException">Thrown for unsupported from clauses</exception>
    internal static ILogicalPlan PlanFromTable(IReadOnlyCollection<TableWithJoins>? tables, IReadOnlyDictionary<string, DataSource> dataSources)
    {
        if (tables == null || tables.Count == 0)
        {
            return new EmptyRelation(true);
        }

        var from = tables.First();
        var tableFactor = from.Relation;

        if (tableFactor is not TableFactor.Table relation)
        {
            throw new InvalidOperationException();
        }

        // Get the table name used to query the data source
        var name = relation.Alias != null ? relation.Alias.Name : relation.Name.Values[0];

        // The root operation will scan the table for the projected values
        var table = dataSources[name];
        return new TableScan(name, table.Schema, table);
    }

    #endregion

    #region Select Plan
    /// <summary>
    /// Builds a logical plan from a query filter
    /// </summary>
    /// <param name="selection">Filter expression</param>
    /// <param name="plan">Input plan</param>
    /// <returns>ILogicalPlan instance to filter the input plan</returns>
    internal static ILogicalPlan PlanFromSelection(Expression? selection, ILogicalPlan plan)
    {
        if (selection == null)
        {
            return plan;
        }

        var filterExpression = SqlToExpr(selection, plan.Schema);
        var usingColumns = new HashSet<Column>();
        Extensions.ExprToColumns(filterExpression, usingColumns);
        //filterExpression = NormalizeColumn(filterExpression, new []{ plan.Schema }, usingColumns);
        return new Filter(plan, filterExpression);
    }
    /// <summary>
    /// Create a projection from a `SELECT` statement
    /// </summary>
    /// <param name="projection"></param>
    /// <param name="plan"></param>
    /// <param name="emptyFrom"></param>
    /// <returns></returns>
    internal static List<ILogicalExpression> PrepareSelectExpressions(IEnumerable<SelectItem> projection, ILogicalPlan plan, bool emptyFrom)
    {
        return projection.Select(SelectToRex).SelectMany(_ => _).ToList();

        List<ILogicalExpression> SelectToRex(SelectItem sql)
        {
            switch (sql)
            {
                case SelectItem.UnnamedExpression u:
                    {
                        var expr = SqlToExpr(u.Expression, plan.Schema);
                        //var column = NormalizeColumn(expr, new []{ plan.Schema }, null);
                        return new List<ILogicalExpression> { expr };
                    }
                case SelectItem.ExpressionWithAlias e:
                    {
                        var select = SqlToExpr(e.Expression, plan.Schema);
                        //var column = NormalizeColumn(select, new[] { plan.Schema }, null);
                        return new List<ILogicalExpression> { new Alias(select, e.Alias) };
                    }
                case SelectItem.Wildcard:
                    if (emptyFrom)
                    {
                        throw new InvalidOperationException("SELECT * with no table is not valid");
                    }

                    return plan.Schema.Fields.Select(f => (ILogicalExpression)new Column(f.Name)).ToList();

                //case SelectItem.QualifiedWildcard q:
                //    return ExpandQualifiedWildcard(qualifier, plan.Schema);

                default:
                    throw new InvalidOperationException("Invalid select expression");
            }
        }

    }

    #endregion

    #region Aggregate Plan
    internal static (ILogicalPlan, List<ILogicalExpression>, List<ILogicalExpression>) CreateAggregatePlan(
        ILogicalPlan plan,
        List<ILogicalExpression> selectExpressions,
        List<ILogicalExpression> havingExpressions,
        List<ILogicalExpression> groupByExpressions,
        List<ILogicalExpression> aggregateExpressions)
    {
        var groupingSets = GroupingSetToExprList(groupByExpressions);
        var allExpressions = groupingSets.Concat(aggregateExpressions).ToList();
        // validate unique names
        var fields = ExprListToFields(allExpressions, plan);
        var schema = new Schema(fields);
        var aggregatePlan = new Aggregate(plan, groupByExpressions, aggregateExpressions, schema);

        var aggregateProjectionExpressions = groupByExpressions.Select(_ => _).Concat(aggregateExpressions).ToList();
        // resolve columns
        aggregateProjectionExpressions = aggregateProjectionExpressions.Select(e => ResolveColumns(e, plan)).ToList();
        aggregateProjectionExpressions = aggregateProjectionExpressions.Select(e => ResolveColumns(e, plan)).ToList();

        var selectExpressionsPostAggregate = selectExpressions.Select(e => RebaseExpr(e, aggregateProjectionExpressions, plan)).ToList();
        // rewrite having columns

        return (aggregatePlan, selectExpressionsPostAggregate, new List<ILogicalExpression>());
    }

    internal static List<ILogicalExpression> GroupingSetToExprList(List<ILogicalExpression> groupExpressions)
    {
        return groupExpressions;
    }

    internal static (AggregateFunctionType, List<ILogicalExpression>) AggregateFunctionToExpr(
        AggregateFunctionType functionType,
        IReadOnlyCollection<FunctionArg>? args,
        Schema schema)
    {
        List<ILogicalExpression> arguments = null!;

        if (functionType == AggregateFunctionType.Count)
        {
            //
        }
        else
        {
            arguments = FunctionArgsToExpr();// args, schema
        }

        return (functionType, arguments);

        List<ILogicalExpression> FunctionArgsToExpr()//IReadOnlyCollection<FunctionArg>? args, Schema schema
        {
            return args == null
                ? new List<ILogicalExpression>()
                : args.Select(SqlFnArgToLogicalExpr).ToList();

            ILogicalExpression SqlFnArgToLogicalExpr(FunctionArg functionArg)
            {
                return functionArg switch
                {
                    FunctionArg.Named { Arg: FunctionArgExpression.Wildcard } => new Wildcard(),
                    FunctionArg.Named { Arg: FunctionArgExpression.FunctionExpression a } => SqlExprToLogicalExpr(a.Expression, schema),
                    FunctionArg.Unnamed { FunctionArgExpression: FunctionArgExpression.FunctionExpression fe } => SqlExprToLogicalExpr(fe.Expression, schema),
                    _ => throw new InvalidOleVariantTypeException($"Unsupported qualified wildcard argument: {functionArg.ToSql()}")
                };
            }
        }
    }

    internal static List<ILogicalExpression> FindGroupByExprs(IReadOnlyCollection<Expression>? selectGroupBy, Schema schema)
    {
        if (selectGroupBy == null)
        {
            return new List<ILogicalExpression>();
        }

        return selectGroupBy.Select(expr =>
        {
            var groupByExpr = SqlExprToLogicalExpr(expr, schema);

            return groupByExpr;
        }).ToList();
    }

    internal static List<ILogicalExpression> FindAggregateExprs(List<ILogicalExpression> expressions)
    {
        return FindNestedExpressions(expressions, nested => nested is AggregateFunction);
    }
    #endregion

    #region Projection Plan
    internal static ILogicalPlan PlanProjection(ILogicalPlan plan, List<ILogicalExpression> expressions)
    {
        var schema = plan.Schema;
        var projectedExpressions = new List<ILogicalExpression>();
        //var fields = new List<Field>();

        foreach (var expr in expressions)
        {
            if (expr is Wildcard)
            {
                // expand
            }
            else if (expr is SelectItem.QualifiedWildcard)
            {
                // expand
            }
            else
            {
                projectedExpressions.Add(ToColumnExpr(expr));
            }
        }

        var fields = ExprListToFields(projectedExpressions, plan);
        return new Projection(plan, expressions, new Schema(fields));

        ILogicalExpression ToColumnExpr(ILogicalExpression expr)
        {
            switch (expr)
            {
                case Column:
                    return expr;

                //case Alias:
                //case Cast
                //case TryCast
                // case ScalarSubQuery
                default:
                    var name = expr.CreateName();
                    var field = schema.GetField(name);
                    return field != null ? expr : new Column(name);
            }
        }

    }

    #endregion

    #region Order By Plan

    internal static ILogicalPlan OrderBy(ILogicalPlan plan, Sequence<OrderByExpression>? orderByExpressions)
    {
        if (orderByExpressions == null || !orderByExpressions.Any())
        {
            return plan;
        }

        //var orderByRex = orderByExpressions!.Select(e => OrderByToSortExpression(e, plan.Schema));

        return plan;
    }

    //private ILogicalExpression OrderByToSortExpression(OrderByExpression orderByExpr, Schema planSchema)
    //{
    //    var expr = 
    //}

    #endregion

    #region Limit Plan
    internal static ILogicalPlan Limit(ILogicalPlan plan, Offset? skip, Expression? fetch)
    {
        if (skip == null && fetch == null)
        {
            return plan;
        }

        return plan;
    }
    #endregion

    #region Helpers
    /// <summary>
    /// Relational expression from sql expression
    /// </summary>
    internal static ILogicalExpression SqlToExpr(Expression predicate, Schema schema)
    {
        var expr = SqlExprToLogicalExpr(predicate, schema);
        // rewrite qualifier
        // validate
        // infer

        return expr;
    }

    internal static ILogicalExpression CloneWithReplacement(ILogicalExpression expr, Func<ILogicalExpression, ILogicalExpression?> replacementFunc)
    {
        var replacementOpt = replacementFunc(expr);

        if (replacementOpt != null)
        {
            return replacementOpt;
        }

        return expr switch
        {
            Column => expr,
            AggregateFunction fn => fn with { Args = fn.Args.Select(_ => CloneWithReplacement(_, replacementFunc)).ToList() },

            _ => throw new NotImplementedException() //todo other types
        };
    }

    public static ILogicalExpression ResolveColumns(ILogicalExpression expr, ILogicalPlan plan)
    {
        return CloneWithReplacement(expr, nested =>
        {
            if (nested is Column c)
            {
                return new Column(plan.Schema.GetField(c.Name)!.Name);
            }

            return null;
        });


    }

    internal static ILogicalExpression ExprAsColumnExpr(ILogicalExpression expr, ILogicalPlan plan)
    {
        if (expr is Column c)
        {
            var field = plan.Schema.GetField(c.Name);
            //TODO qualified name
            return new Column(field!.Name);
        }

        return new Column(expr.CreateName());
    }

    internal static ILogicalExpression RebaseExpr(ILogicalExpression expr, ICollection<ILogicalExpression> baseExpressions, ILogicalPlan plan)
    {
        return CloneWithReplacement(expr, nested => baseExpressions.Contains(nested) ? ExprAsColumnExpr(nested, plan) : null);
    }


    internal static ILogicalExpression SqlExprToLogicalExpr(Expression predicate, Schema schema)
    {
        if (predicate is Expression.BinaryOp b)
        {
            return ParseSqlBinaryOp(b.Left, b.Op, b.Right, schema);
        }

        return SqlExprToLogicalInternal(predicate, schema);
    }

    internal static ILogicalExpression ParseSqlBinaryOp(Expression left, BinaryOperator op, Expression right, Schema schema)
    {
        return new BinaryExpr(SqlExprToLogicalExpr(left, schema), op, SqlExprToLogicalExpr(right, schema));
    }

    internal static ILogicalExpression SqlExprToLogicalInternal(Expression expr, Schema schema)
    {
        switch (expr)
        {
            case Expression.LiteralValue v:
                return ParseValue(v);

            case Expression.Identifier ident:
                return SqlIdentifierToExpr(ident, schema);

            case Expression.Function fn:
                return SqlFunctionToExpr(fn, schema);

            default:
                throw new NotImplementedException();
        }
    }

    internal static ILogicalExpression SqlIdentifierToExpr(Expression.Identifier ident, Schema schema)
    {
        return new Column(schema.GetField(ident.Ident.Value)!.Name);
    }


    internal static ILogicalExpression ParseValue(Expression.LiteralValue literalValue)
    {
        switch (literalValue.Value)
        {
            //TODO case Value.Null: literal scalar value

            case Value.Number n:
                return ParseSqlNumber(n);

            case Value.SingleQuotedString sq:
                return new LiteralExpression(new StringScalarValue(sq.Value));

            case Value.Boolean b:
                return new LiteralExpression(new BooleanScalarValue(b.Value));

            default:
                throw new NotImplementedException();
        }
    }

    internal static ILogicalExpression SqlFunctionToExpr(Expression.Function function, Schema schema)
    {
        // scalar functions

        // aggregate functions
        var name = function.Name;

        var aggregateType = AggregateFunction.GetFunctionType(name);
        if (aggregateType.HasValue)
        {
            var distinct = function.Distinct;

            var (aggregateFunction, expressionArgs) = AggregateFunctionToExpr(aggregateType.Value, function.Args, schema);
            return new AggregateFunction(aggregateFunction, expressionArgs, distinct);
        }

        throw new InvalidOperationException("Invalid function");
    }

    internal static ILogicalExpression ParseSqlNumber(Value.Number number)
    {
        if (long.TryParse(number.Value, out var parsedInt))
        {
            return new LiteralExpression(new IntegerScalarValue(parsedInt));
        }

        if (float.TryParse(number.Value, out var parsedFloat))
        {
            return new LiteralExpression(new FloatScalarValue(parsedFloat));
        }

        return new LiteralExpression(new StringScalarValue(number.Value));
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
            if (predicate((ILogicalExpression)e))
            {
                if (!expressions.Contains(e))
                {
                    expressions.Add((ILogicalExpression)e);
                }

                return VisitRecursion.Skip;
            }

            return VisitRecursion.Continue;
        });

        return expressions;
    }
    #endregion

    #region Physical Expression
    internal static IPhysicalExpression CreatePhysicalExpr(ILogicalExpression expression, Schema inputDfSchema, Schema inputSchema)
    {
        switch (expression)
        {
            case Column c:
                var index = inputDfSchema.IndexOfColumn(c);
                return new PhysicalColumn(c.Name, index!.Value);

            case BinaryExpr b:
                {
                    var left = CreatePhysicalExpr(b.Left, inputDfSchema, inputSchema);
                    var right = CreatePhysicalExpr(b.Right, inputDfSchema, inputSchema);

                    return new PhysicalBinaryExpr(left, b.Op, right);
                }
            case LiteralExpression l:
                return new Literal(l.Value);

            default:
                throw new NotImplementedException($"Expression type {expression.GetType().Name} is not yet supported.");
        }
    }

    internal static string GetPhysicalName(ILogicalExpression expr)
    {
        return expr switch
        {
            Column c => c.Name,
            BinaryExpr b => $"{GetPhysicalName(b.Left)} {b.Op} {GetPhysicalName(b.Right)}",
            AggregateFunction fn => CreateFunctionPhysicalName(fn, fn.Distinct, fn.Args),
            _ => throw new NotImplementedException()
        };

       
    }

    internal static string CreateFunctionPhysicalName(AggregateFunction fn, bool distinct, List<ILogicalExpression> args)
    {
        var names = args.Select(e => CreatePhysicalName(e, false)).ToList();

        var distinctText = distinct ? "DISTINCT " : "";

        return $"{fn.FunctionType}({distinctText}{string.Join(",", names)})";
    }

    internal static string CreatePhysicalName(ILogicalExpression expression, bool isFirst)
    {
        switch (expression)
        {
            case Column c:
                return c.Name;//todo is first?name:flatname

            case BinaryExpr b:
                return $"{CreatePhysicalName(b.Left, false)} {b.Op} {CreatePhysicalName(b.Left, false)}";

            case AggregateFunction fn:
                return CreateFunctionPhysicalName(fn, fn.Distinct, fn.Args);

            default:
                throw new NotImplementedException();
        }
    }

    #endregion
}
