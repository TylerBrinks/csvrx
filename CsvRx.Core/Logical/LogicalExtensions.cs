using System.Runtime.InteropServices;
using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;
using CsvRx.Core.Logical.Values;
using CsvRx.Core.Physical.Expressions;
using SqlParser.Ast;
using Aggregate = CsvRx.Core.Logical.Plans.Aggregate;
using Binary = CsvRx.Core.Physical.Expressions.Binary;
using Column = CsvRx.Core.Physical.Expressions.Column;
using Literal = CsvRx.Core.Logical.Expressions.Literal;

namespace CsvRx.Core.Logical;

internal static class LogicalExtensions
{
    internal static string CreateName(this ILogicalExpression expression)
    {
        return expression switch
        {
            Alias a => a.Name, 
            Expressions.Column c => c.Name,
            Expressions.Binary b => $"{b.Left.CreateName()} {b.Op} {b.Right.CreateName()}",
            AggregateFunction fn => GetFunctionName(fn, false, fn.Args),
            Literal l => l.Value.RawValue.ToString(),
            //Like
            // Case
            // cast
            // not
            // is null
            // is not null
            Wildcard => "*",
            _ => throw new NotImplementedException("need to implement")
        };

        string GetFunctionName(AggregateFunction fn, bool distinct, List<ILogicalExpression> args)
        {
            var names = args.Select(CreateName).ToList();
            var distinctName = distinct ? "DISTINCT " : string.Empty;
            var functionName = fn.FunctionType.ToString().ToUpperInvariant();
            return $"{functionName}({distinctName}{string.Join(",", names)})";
        }
    }

    internal static void ExprListToColumns(List<ILogicalExpression> expressions, HashSet<Expressions.Column> accumulator)
    {
        foreach (var expr in expressions)
        {
            ExpressionToColumns(expr, accumulator);
        }
    }

    internal static void ExpressionToColumns(ILogicalExpression expression, HashSet<Expressions.Column> accumulator)
    {
        InspectExprPre(expression, Inspect);

        void Inspect(ILogicalExpression expr)
        {
            switch (expr)
            {
                case Expressions.Column col:
                    accumulator.Add(col);
                    break;

                case ScalarVariable sv:
                    accumulator.Add(new Expressions.Column(string.Join(".", sv.Names)));
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
        var fields = expressions.Select(e => ToField(e, plan.Schema)).ToList();
        
        return fields;
    }

    internal static Field ToField(ILogicalExpression expression, Schema schema)
    {
        if (expression is not Expressions.Column c)
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
            Expressions.Column c => schema.GetField(c.Name)!.DataType,
            Alias a => GetDataType(a.Expression, schema),
            AggregateFunction fn => GetAggregateDataType(fn),

            _ => throw new NotImplementedException(),
        };

        ColumnDataType GetAggregateDataType(AggregateFunction function)
        {
            var dataTypes = function.Args.Select(e => GetDataType(e, schema)).ToList();

            return function.FunctionType switch
            {
                AggregateFunctionType.Min or AggregateFunctionType.Max  => CoercedTypes(function, dataTypes),
                AggregateFunctionType.Sum => ColumnDataType.Integer,
                AggregateFunctionType.Count => ColumnDataType.Integer,
                AggregateFunctionType.Avg => ColumnDataType.Double,

                _ => throw new NotImplementedException("need to implement"),
            };
        }

        ColumnDataType CoercedTypes(AggregateFunction function, IReadOnlyList<ColumnDataType> inputTypes)
        {
            return function.FunctionType switch
            {
                AggregateFunctionType.Min or AggregateFunctionType.Max => GetMinMaxType(),
                _ => throw new NotImplementedException("need to implement"),
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

    internal static void MergeSchemas(this Schema self, Schema second)
    {
        if (!second.Fields.Any())
        {
            return;
        }

        // TODO null fields?
        foreach (var field in second.Fields.Where(f => f != null))
        {
            var duplicate = self.Fields.FirstOrDefault(f => f != null && f.Name == field!.Name) != null;

            if (!duplicate)
            {
                self.Fields.Add(field);
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

        var filterExpression = SqlToExpression(selection, plan.Schema);
        var usingColumns = new HashSet<Expressions.Column>();
        ExpressionToColumns(filterExpression, usingColumns);
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
        return projection.Select(SelectToRelationalExpression).SelectMany(_ => _).ToList();

        List<ILogicalExpression> SelectToRelationalExpression(SelectItem sql)
        {
            switch (sql)
            {
                case SelectItem.UnnamedExpression u:
                    {
                        var expr = SqlToExpression(u.Expression, plan.Schema);
                        return new List<ILogicalExpression> { expr };
                    }
                case SelectItem.ExpressionWithAlias e:
                    {
                        var select = SqlToExpression(e.Expression, plan.Schema);
                        return new List<ILogicalExpression> { new Alias(select, e.Alias) };
                    }
                case SelectItem.Wildcard:
                    if (emptyFrom)
                    {
                        throw new InvalidOperationException("SELECT * with no table is not valid");
                    }

                    return plan.Schema.Fields.Select(f => (ILogicalExpression)new Expressions.Column(f.Name)).ToList();

                default:
                    throw new InvalidOperationException("Invalid select expression");
            }
        }

    }

    internal static List<ILogicalExpression> ExpandWildcard(Schema schema)
    {
        // todo using columns for join
        return schema.Fields.Select(f => (ILogicalExpression)new Expressions.Column(f.Name)).ToList();
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

        //var aggregateProjectionExpressions = groupByExpressions.Select(_ => _).Concat(aggregateExpressions).ToList();
        var aggregateProjectionExpressions = groupByExpressions.ToList().Concat(aggregateExpressions).ToList();
        // resolve columns
        aggregateProjectionExpressions = aggregateProjectionExpressions.Select(e => ResolveColumns(e, plan)).ToList();
        aggregateProjectionExpressions = aggregateProjectionExpressions.Select(e => ResolveColumns(e, plan)).ToList();

        var selectExpressionsPostAggregate = selectExpressions.Select(e => RebaseExpression(e, aggregateProjectionExpressions, plan)).ToList();
        // rewrite having columns

        return (aggregatePlan, selectExpressionsPostAggregate, new List<ILogicalExpression>());
    }

    internal static List<ILogicalExpression> GroupingSetToExprList(List<ILogicalExpression> groupExpressions)
    {
        return groupExpressions;
    }

    internal static (AggregateFunctionType, List<ILogicalExpression>) AggregateFunctionToExpression(
        AggregateFunctionType functionType,
        IReadOnlyCollection<FunctionArg>? args,
        Schema schema)
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
                        => SqlExprToLogicalExpression(a.Expression, schema),
                    FunctionArg.Unnamed { FunctionArgExpression: FunctionArgExpression.FunctionExpression fe } 
                        => SqlExprToLogicalExpression(fe.Expression, schema),
                    
                    _ => throw new InvalidOleVariantTypeException($"Unsupported qualified wildcard argument: {functionArg.ToSql()}")
                };
            }
        }
    }

    internal static List<ILogicalExpression> FindGroupByExpressions(IReadOnlyCollection<Expression>? selectGroupBy, Schema combinedSchema/*, Schema planSchema*/)
    {
        if (selectGroupBy == null)
        {
            return new List<ILogicalExpression>();
        }

        return selectGroupBy.Select(expr =>
        {
            var groupByExpr = SqlExprToLogicalExpression(expr, combinedSchema);

            // resolve aliases

            return groupByExpr;
        }).ToList();
    }

    internal static List<ILogicalExpression> FindAggregateExpressions(List<ILogicalExpression> expressions)
    {
        return FindNestedExpressions(expressions, nested => nested is AggregateFunction);
    }
    #endregion

    #region Projection Plan
    internal static ILogicalPlan PlanProjection(ILogicalPlan plan, List<ILogicalExpression> expressions)
    {
        var projectedExpressions = new List<ILogicalExpression>();

        foreach (var expr in expressions)
        {
            if (expr is Wildcard)
            {
                // TODO: expand
            }
            else if (expr is SelectItem.QualifiedWildcard)
            {
                // TODO: expand
            }
            else
            {
                projectedExpressions.Add(ToColumnExpression(expr, plan.Schema));
            }
        }

        var fields = ExprListToFields(projectedExpressions, plan);
        return new Projection(plan, expressions, new Schema(fields));

        static ILogicalExpression ToColumnExpression(ILogicalExpression expression, Schema schema)
        {
            switch (expression)
            {
                case Expressions.Column:
                    return expression;

                case Alias alias:
                    return alias with { Expression = ToColumnExpression(alias.Expression, schema) };

;               //case Cast
                //case TryCast
                // case ScalarSubQuery

                default:
                    var name = expression.CreateName();
                    var field = schema.GetField(name);
                    return field == null ? expression : new Expressions.Column(name);
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

        var orderByRelation = orderByExpressions!.Select(e => OrderByToSortExpression(e, plan.Schema)).ToList();

        return Sort.TryNew(plan, orderByRelation);
    }

    private static ILogicalExpression OrderByToSortExpression(OrderByExpression orderByExpression, Schema schema)
    {
        var expr = SqlExprToLogicalExpression(orderByExpression.Expression, schema);

        return new OrderBy(expr, orderByExpression.Asc ?? true);
    }

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
    internal static ILogicalExpression SqlToExpression(Expression predicate, Schema schema)
    {
        var expr = SqlExprToLogicalExpression(predicate, schema);
        // rewrite qualifier
        // validate
        // infer

        return expr;
    }

    internal static ILogicalExpression CloneWithReplacement(ILogicalExpression expression, Func<ILogicalExpression, ILogicalExpression?> replacementFunc)
    {
        var replacementOpt = replacementFunc(expression);

        if (replacementOpt != null)
        {
            return replacementOpt;
        }

        return expression switch
        {
            Expressions.Column => expression,
            AggregateFunction fn => fn with { Args = fn.Args.Select(_ => CloneWithReplacement(_, replacementFunc)).ToList() },

            _ => throw new NotImplementedException() //todo other types
        };
    }

    public static ILogicalExpression ResolveColumns(ILogicalExpression expression, ILogicalPlan plan)
    {
        return CloneWithReplacement(expression, nested =>
        {
            if (nested is Expressions.Column c)
            {
                return new Expressions.Column(plan.Schema.GetField(c.Name)!.Name);
            }

            return null;
        });


    }

    internal static ILogicalExpression ExpressionAsColumn(ILogicalExpression expression, ILogicalPlan plan)
    {
        if (expression is Expressions.Column c)
        {
            var field = plan.Schema.GetField(c.Name);
            //TODO qualified name
            return new Expressions.Column(field!.Name);
        }

        return new Expressions.Column(expression.CreateName());
    }

    internal static ILogicalExpression RebaseExpression(ILogicalExpression expression, ICollection<ILogicalExpression> baseExpressions, ILogicalPlan plan)
    {
        return CloneWithReplacement(expression, nested => baseExpressions.Contains(nested) ? ExpressionAsColumn(nested, plan) : null);
    }


    internal static ILogicalExpression SqlExprToLogicalExpression(Expression predicate, Schema schema)
    {
        if (predicate is Expression.BinaryOp b)
        {
            return ParseSqlBinaryOp(b.Left, b.Op, b.Right, schema);
        }

        return SqlExprToLogicalInternal(predicate, schema);
    }

    internal static ILogicalExpression ParseSqlBinaryOp(Expression left, BinaryOperator op, Expression right, Schema schema)
    {
        return new Expressions.Binary(SqlExprToLogicalExpression(left, schema), op, SqlExprToLogicalExpression(right, schema));
    }

    internal static ILogicalExpression SqlExprToLogicalInternal(Expression expression, Schema schema)
    {
        switch (expression)
        {
            case Expression.LiteralValue v:
                return ParseValue(v);

            case Expression.Identifier ident:
                return SqlIdentifierToExpression(ident, schema);

            case Expression.Function fn:
                return SqlFunctionToExpression(fn, schema);

            default:
                throw new NotImplementedException();
        }
    }

    internal static ILogicalExpression SqlIdentifierToExpression(Expression.Identifier ident, Schema schema)
    {
        return new Expressions.Column(schema.GetField(ident.Ident.Value)!.Name);
    }


    internal static ILogicalExpression ParseValue(Expression.LiteralValue literalValue)
    {
        switch (literalValue.Value)
        {
            //TODO case Value.Null: literal scalar value

            case Value.Number n:
                return ParseSqlNumber(n);

            case Value.SingleQuotedString sq:
                return new Literal(new StringScalar(sq.Value));

            case Value.Boolean b:
                return new Literal(new BooleanScalar(b.Value));

            default:
                throw new NotImplementedException();
        }
    }

    internal static ILogicalExpression SqlFunctionToExpression(Expression.Function function, Schema schema)
    {
        // scalar functions

        // aggregate functions
        var name = function.Name;

        var aggregateType = AggregateFunction.GetFunctionType(name);
        if (aggregateType.HasValue)
        {
            var distinct = function.Distinct;

            var (aggregateFunction, expressionArgs) = AggregateFunctionToExpression(aggregateType.Value, function.Args, schema);
            return new AggregateFunction(aggregateFunction, expressionArgs, distinct);
        }

        throw new InvalidOperationException("Invalid function");
    }

    internal static ILogicalExpression ParseSqlNumber(Value.Number number)
    {
        if (long.TryParse(number.Value, out var parsedInt))
        {
            return new Literal(new IntegerScalar(parsedInt));
        }

        if (double.TryParse(number.Value, out var parsedDouble))
        {
            return new Literal(new DoubleScalar(parsedDouble));
        }

        return new Literal(new StringScalar(number.Value));
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
            if (!predicate((ILogicalExpression) e))
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
    #endregion

    #region Physical Expression

    internal static IPhysicalExpression CreatePhysicalExpression(ILogicalExpression expression, Schema inputDfSchema, Schema inputSchema)
    {
        while (true)
        {
            switch (expression)
            {
                case Expressions.Column c:
                    var index = inputDfSchema.IndexOfColumn(c);
                    return new Column(c.Name, index!.Value);

                case Literal l:
                    return new Physical.Expressions.Literal(l.Value);

                case Alias a:
                    expression = a.Expression;
                    continue;

                case Expressions.Binary b:
                {
                    var left = CreatePhysicalExpression(b.Left, inputDfSchema, inputSchema);
                    var right = CreatePhysicalExpression(b.Right, inputDfSchema, inputSchema);

                    return new Binary(left, b.Op, right);
                }

                default:
                    throw new NotImplementedException($"Expression type {expression.GetType().Name} is not yet supported.");
            }
        }
    }

    internal static string GetPhysicalName(ILogicalExpression expression)
    {
        return expression switch
        {
            Expressions.Column c => c.Name,
            Expressions.Binary b => $"{GetPhysicalName(b.Left)} {b.Op} {GetPhysicalName(b.Right)}",
            Alias a => a.Name,
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
            case Expressions.Column c:
                return c.Name;//todo is first?name:flatname

            case Expressions.Binary b:
                return $"{CreatePhysicalName(b.Left, false)} {b.Op} {CreatePhysicalName(b.Left, false)}";

            case AggregateFunction fn:
                return CreateFunctionPhysicalName(fn, fn.Distinct, fn.Args);

            default:
                throw new NotImplementedException();
        }
    }

    #endregion
}
