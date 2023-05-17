using System.Runtime.InteropServices;
using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;
using CsvRx.Core.Logical.Values;
using CsvRx.Core.Physical.Expressions;
using SqlParser.Ast;
using static SqlParser.Ast.Expression;
using Aggregate = CsvRx.Core.Logical.Plans.Aggregate;
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
        return expression switch
        {
            Alias a => a.Name,
            Column c => c.FlatName,
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

        static string GetFunctionName(AggregateFunction fn, bool distinct, List<ILogicalExpression> args)
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
                    accumulator.Add(Column.FromName(string.Join(".", sv.Names)));
                    break;
            }
        }
    }
    /// <summary>
    /// Converts a list of expressions into a list of qualified fields
    /// </summary>
    /// <param name="expressions">Logical expressions to iterate</param>
    /// <param name="plan"></param>
    /// <returns></returns>
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
        if (expression is Column c)
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

        //left = ParseRelationJoin(left, table.Joins);

        foreach (var join in table.Joins)
        {
            left = left.ParseRelationJoin(join, context);
        }

        return left;
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

        if (tableRef?.Alias == null)
        {
            return plan;
        }

        return SubqueryAlias.TryNew(plan, tableRef.Alias);
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
        var expression = on.Expression.SqlToExpression(joinSchema);
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
    /// <returns>ILogicalPlan instance to filter the input plan</returns>
    internal static ILogicalPlan PlanFromSelection(this Expression? selection, ILogicalPlan plan)
    {
        if (selection == null)
        {
            return plan;
        }

        var filterExpression = selection.SqlToExpression(plan.Schema);
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
    /// <returns></returns>
    internal static List<ILogicalExpression> PrepareSelectExpressions(this IEnumerable<SelectItem> projection,
        ILogicalPlan plan, bool emptyFrom)
    {
        return projection.Select(SelectToRelationalExpression).SelectMany(_ => _).ToList();

        List<ILogicalExpression> SelectToRelationalExpression(SelectItem sql)
        {
            switch (sql)
            {
                case SelectItem.UnnamedExpression u:
                    {
                        var expr = u.Expression.SqlToExpression(plan.Schema);
                        var column = expr.NormalizeColumnWithSchemas(plan.Schema.AsNested(), plan.UsingColumns);
                        return new List<ILogicalExpression> { column };
                    }
                case SelectItem.ExpressionWithAlias e:
                    {
                        var select = e.Expression.SqlToExpression(plan.Schema);
                        var column = select.NormalizeColumnWithSchemas(plan.Schema.AsNested(), plan.UsingColumns);
                        return new List<ILogicalExpression> { new Alias(column, e.Alias) };
                    }
                case SelectItem.Wildcard:
                    if (emptyFrom)
                    {
                        throw new InvalidOperationException("SELECT * with no table is not valid");
                    }

                    //TODO expand wildcard select.rs line 320
                    return plan.Schema.ExpandWildcard(plan);

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
        List<HashSet<Column>> usingColumns)
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
    /// <param name="plan">Logical plan</param>
    /// <returns>List of expressions expanded from the wildcard</returns>
    internal static List<ILogicalExpression> ExpandWildcard(this Schema schema, ILogicalPlan plan)
    {
        // todo using columns for join
        //TODO qualified columns
        return schema.Fields.Select(f => (ILogicalExpression)f.QualifiedColumn()).ToList();
    }

    #endregion

    #region Aggregate Plan
    /// <summary>
    /// Crete an aggregate plan from a list of SELECT, HAVING, GROUP BY, and Aggregate expressions
    /// </summary>
    /// <param name="plan">Logical plan to wrap</param>
    /// <param name="selectExpressions">Select expression list</param>
    /// <param name="havingExpressions">Query HAVING expression list</param>
    /// <param name="groupByExpressions">Query GROUP BY expression list</param>
    /// <param name="aggregateExpressions">Aggregate function expression list</param>
    /// <returns></returns>
    internal static (ILogicalPlan, List<ILogicalExpression>, ILogicalExpression?) CreateAggregatePlan(
        this ILogicalPlan plan,
        List<ILogicalExpression> selectExpressions,
        ILogicalExpression? havingExpressions,
        List<ILogicalExpression> groupByExpressions,
        List<ILogicalExpression> aggregateExpressions)
    {
        var groupingSets = groupByExpressions;// GroupingSetToExprList(groupByExpressions);
        var allExpressions = groupingSets.Concat(aggregateExpressions).ToList();

        var fields = allExpressions.ExpressionListToFields(plan);
        var schema = new Schema(fields);
        var aggregatePlan = new Aggregate(plan, groupByExpressions, aggregateExpressions, schema);

        var aggregateProjectionExpressions = groupByExpressions
            .ToList()
            .Concat(aggregateExpressions)
            .Select(e => ResolveColumns(e, plan))
            .ToList();

        // resolve columns and replace with fully qualified names
        //aggregateProjectionExpressions = aggregateProjectionExpressions.Select(e => ResolveColumns(e, plan)).ToList();

        //var columnExpressionsPostAggregate = aggregateProjectionExpressions.Select(e => ExpressionAsColumn(e, plan)).ToList();

        var selectExpressionsPostAggregate = selectExpressions.Select(e => RebaseExpression(e, aggregateProjectionExpressions, plan)).ToList();

        // rewrite having columns to use columns in by the aggregation
        ILogicalExpression? havingPostAggregation = null;

        if (havingExpressions != null)
        {
            havingPostAggregation = RebaseExpression(havingExpressions, aggregateProjectionExpressions, plan);
        }

        return (aggregatePlan, selectExpressionsPostAggregate, havingPostAggregation);
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

    internal static List<ILogicalExpression> FindGroupByExpressions(
        this IReadOnlyCollection<Expression>? selectGroupBy,
        List<ILogicalExpression> selectExpressions,
        Schema combinedSchema,
        ILogicalPlan plan,
        Dictionary<string, ILogicalExpression> aliasMap)
    {
        if (selectGroupBy == null)
        {
            return new List<ILogicalExpression>();
        }

        return selectGroupBy.Select(expr =>
        {
            var groupByExpr = SqlExprToLogicalExpression(expr, combinedSchema);

            foreach (var field in plan.Schema.Fields/*.Where(f => f != null)*/)
            {
                aliasMap.Remove(field.Name);//field!.Name
            }

            groupByExpr = groupByExpr.ResolveAliasToExpressions(aliasMap);
            groupByExpr = groupByExpr.ResolvePositionsToExpressions(selectExpressions) ?? groupByExpr;
            groupByExpr = groupByExpr.NormalizeColumn(plan);
            return groupByExpr;
        }).ToList();
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

    internal static ILogicalExpression? MapHaving(this Expression? having, Schema schema, Dictionary<string, ILogicalExpression> aliasMap)
    {
        var havingExpression = having == null ? null : SqlExprToLogicalExpression(having, schema);

        return havingExpression == null ? null :
            // This step swaps aliases in the HAVING clause for the
            // underlying column name.  This is how the planner supports
            // queries with HAVING expressions that refer to aliased columns.
            //
            //   SELECT c1, MAX(c2) AS abc FROM tbl GROUP BY c1 HAVING abc > 10;
            //
            // is rewritten
            //
            //   SELECT c1, MAX(c2) AS abc FROM tbl GROUP BY c1 HAVING MAX(c2) > 10;
            havingExpression.ResolveAliasToExpressions(aliasMap);
    }

    internal static ILogicalExpression ResolveAliasToExpressions(this ILogicalExpression expression,
        IReadOnlyDictionary<string, ILogicalExpression> aliasMap)
    {
        return CloneWithReplacement(expression, e =>
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

    #endregion

    #region Order By Plan
    internal static ILogicalPlan OrderBy(this ILogicalPlan plan, Sequence<OrderByExpression>? orderByExpressions)
    {
        if (orderByExpressions == null || !orderByExpressions.Any())
        {
            return plan;
        }

        var orderByRelation = orderByExpressions.Select(e => e.OrderByToSortExpression(plan.Schema)).ToList();

        return Sort.TryNew(plan, orderByRelation);
    }

    private static ILogicalExpression OrderByToSortExpression(this OrderByExpression orderByExpression, Schema schema)
    {
        var expr = SqlExprToLogicalExpression(orderByExpression.Expression, schema);

        return new OrderBy(expr, orderByExpression.Asc ?? true);
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

        var fields = joinType switch
        {
            JoinType.Inner
                or JoinType.Full
                or JoinType.Right => leftFields.Concat(rightFields).ToList(),

            JoinType.Left => GetLeftJoinFields(),

            JoinType.LeftSemi or JoinType.LeftAnti => leftFields,
            JoinType.RightSemi or JoinType.RightAnti => rightFields
            // No default branch; all options accounted for.
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

                case Expressions.Binary b:
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
            Expressions.Binary b => $"{b.Left.GetPhysicalName()} {b.Op} {b.Right.GetPhysicalName()}",
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

            case Expressions.Binary b:
                return $"{CreatePhysicalName(b.Left, false)} {b.Op} {CreatePhysicalName(b.Left, false)}";

            case AggregateFunction fn:
                return fn.CreateFunctionPhysicalName(fn.Distinct, fn.Args);

            default:
                throw new NotImplementedException();
        }
    }

    #endregion

    #region Helpers

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

        // TODO null fields?
        foreach (var field in fromSchema.Fields/*.Where(f => f != null)*/)
        {
            var duplicate = self.Fields.FirstOrDefault(f => /*f != null && */ f.Name == field.Name) != null;//field!.Name

            if (!duplicate)
            {
                self.Fields.Add(field);
            }
        }
    }
    /// <summary>
    /// Relational expression from sql expression
    /// </summary>
    internal static ILogicalExpression SqlToExpression(this Expression predicate, Schema schema)
    {
        var expr = SqlExprToLogicalExpression(predicate, schema);
        // rewrite qualifier
        // validate
        // infer

        return expr;
    }

    internal static ILogicalExpression CloneWithReplacement(
        ILogicalExpression expression,
        Func<ILogicalExpression, ILogicalExpression?> replacementFunc)
    {
        var replacementOpt = replacementFunc(expression);

        if (replacementOpt != null)
        {
            return replacementOpt;
        }

        return expression switch
        {
            Column or Literal => expression,
            AggregateFunction fn => fn with { Args = fn.Args.Select(_ => CloneWithReplacement(_, replacementFunc)).ToList() },
            Alias a => a with { Expression = CloneWithReplacement(a.Expression, replacementFunc) },

            Expressions.Binary b => new Expressions.Binary(
                    CloneWithReplacement(b.Left, replacementFunc), b.
                    Op, CloneWithReplacement(b.Right,
                    replacementFunc)),

            _ => throw new NotImplementedException() //todo other types
        };
    }

    public static ILogicalExpression ResolveColumns(ILogicalExpression expression, ILogicalPlan plan)
    {
        return CloneWithReplacement(expression, nested =>
        {
            if (nested is Column c)
            {
                return plan.Schema.GetField(c.Name)!.QualifiedColumn();
            }

            return null;
        });


    }

    internal static ILogicalExpression RebaseExpression(
        ILogicalExpression expression,
        ICollection<ILogicalExpression> baseExpressions,
        ILogicalPlan plan)
    {
        return CloneWithReplacement(expression, nested =>
        {
            if (baseExpressions.Contains(nested))
            {
                return ExpressionAsColumn(nested, plan);
            }
            //return baseExpressions.Contains(nested) ? ExpressionAsColumn(nested, plan) : null;
            return null;
        });

        static ILogicalExpression ExpressionAsColumn(ILogicalExpression expression, ILogicalPlan plan)
        {
            if (expression is not Column c)
            {
                return Column.FromName(expression.CreateName());
            }

            var field = plan.Schema.GetField(c.Name);
            return field!.QualifiedColumn();
        }
    }

    internal static ILogicalExpression SqlExprToLogicalExpression(Expression predicate, Schema schema)
    {
        if (predicate is BinaryOp b)
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
            case LiteralValue v:
                return ParseValue(v);

            case Identifier ident:
                return SqlIdentifierToExpression(ident, schema);

            case Function fn:
                return SqlFunctionToExpression(fn, schema);

            case CompoundIdentifier ci:
                return SqlCompoundIdentToExpression(ci, schema);

            default:
                throw new NotImplementedException();
        }
    }

    internal static ILogicalExpression SqlIdentifierToExpression(Identifier ident, Schema schema)
    {
        // Found a match without a qualified name, this is a inner table column
        return new Column(schema.GetField(ident.Ident.Value)!.Name, null);
    }

    internal static ILogicalExpression ParseValue(LiteralValue literalValue)
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

    internal static ILogicalExpression SqlFunctionToExpression(Function function, Schema schema)
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

    internal static ILogicalExpression SqlCompoundIdentToExpression(CompoundIdentifier ident, Schema schema)
    {
        if (ident.Idents.Count > 2)
        {
            throw new InvalidOperationException("Not a valid compound identifier");
        }

        var terms = GenerateSearchTerms(ident.Idents.Select(i => i.Value).ToList()).ToList();

        var result = terms.Select(term => new
        {
            Term = term,
            Field = schema.GetQualifiedField(term.Table, term.ColumnName)
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

    internal static List<List<Schema>> AsNested(this Schema schema)
    {
        return new List<List<Schema>> { new() { schema } };
    }

    internal static List<HashSet<Column>> AsNested(this HashSet<Column> usingColumns)
    {
        return new List<HashSet<Column>> { usingColumns };
    }

    internal static List<Schema> FallbackNormalizeSchemas(this ILogicalPlan schema)
    {
        return schema switch
        {
            Projection
                or Aggregate
                or Join => schema.GetInputs().Select(input => input.Schema).ToList(),

            // TODO or Crossjoin

            _ => new List<Schema>()
        };
    }

    private static List<(TableReference? Table, string ColumnName, string[] NestedNames)>
        GenerateSearchTerms(IReadOnlyCollection<string> idents)
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
}