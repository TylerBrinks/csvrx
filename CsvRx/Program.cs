using CsvRx.Data;
using CsvRx.Logical;
using CsvRx.Logical.Expressions;
using CsvRx.Logical.Functions;
using SqlParser.Ast;
using System.Runtime.InteropServices;

var context = new ExecutionContext();
// ReSharper disable once StringLiteralTypo
context.RegisterCsv("aggregate_test_100", @"C:\Users\tyler\source\repos\sink\sqldatafusion\testing\data\csv\aggregate_test_100.csv");
var df = context.Sql("SELECT c1, MAX(c3) FROM aggregate_test_100 WHERE c11 > 0.1 AND c11 < 0.9 GROUP BY c1");
//var df = context.Sql("SELECT MAX(c3) FROM aggregate_test_100");
//context.Execute(df);
Console.Write(df.ToStringIndented(new Indentation()));

public class Planner
{
    public ILogicalPlan CreateLogicalPlan(Query query, Dictionary<string, DataSource> dataSources)
    {
        var select = query.Body.AsSelect();

        var plan = PlanFromTable(select.From, dataSources);

        // Wrap the scan in a filter if a clause exists
        if (select.Selection != null)
        {
            plan = PlanSelection(select.Selection, plan);
        }

        var emptyFrom = plan is EmptyRelation;
        var selectExpressions = PrepareSelectExpressions(select.Projection, plan, emptyFrom);

        // having and group by may reference columns in the projection
        // validate schema satisfies exprs

        plan = new Projection(plan, selectExpressions, plan.Schema);
        //ILogicalPlan projectedPlan = Project(plan, selectExpressions);

        return plan;
    }


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
    private static ILogicalPlan PlanFromTable(IReadOnlyCollection<TableWithJoins>? tables, IReadOnlyDictionary<string, DataSource> dataSources)
    {
        if (tables == null || tables.Count == 0)
        {
            return new EmptyRelation(true);
        }

        var from = tables!.First();
        var tableFactor = from.Relation;

        if (tableFactor is not TableFactor.Table relation)
        {
            throw new InvalidOperationException();
        }

        // Get the table name used to query the data source
        var name = relation.Alias != null ? relation.Alias.Name : relation.Name.Values[0];

        // The root operation will scan the table for the projected values
        var table = dataSources[name];
        return new TableScan(name, table);
    }
    /// <summary>
    /// Builds a logical plan from a query filter
    /// </summary>
    /// <param name="predicate">Filter expression</param>
    /// <param name="plan">Input plan</param>
    /// <returns>ILogicalPlan instance to filter the input plan</returns>
    private ILogicalPlan PlanSelection(Expression predicate, ILogicalPlan plan)
    {
        var filterExpression = SqlToExpr(predicate, plan.Schema);
        var usingColumns = new HashSet<Column>();
        ExprToColumns(filterExpression, usingColumns);
        //filterExpression = NormalizeColumn(filterExpression, new []{ plan.Schema }, usingColumns);
        return new Filter(filterExpression, plan);
    }
    /// <summary>
    /// Create a projection from a `SELECT` statement
    /// </summary>
    /// <param name="projection"></param>
    /// <param name="plan"></param>
    /// <param name="emptyFrom"></param>
    /// <returns></returns>
    private List<ILogicalExpression> PrepareSelectExpressions(Sequence<SelectItem> projection, ILogicalPlan plan, bool emptyFrom)
    {
        return projection.Select(expr => SelectToRex(expr, plan, emptyFrom)).SelectMany(_ => _).ToList();
    }

    private List<ILogicalExpression> SelectToRex(SelectItem sql, ILogicalPlan plan, bool emptyFrom)
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
            //return ExpandWildcard(plan.Schema);//, plan);

            //case SelectItem.QualifiedWildcard q:
            //    return ExpandQualifiedWildcard(qualifier, plan.Schema);

            default:
                throw new InvalidOperationException("Invalid select expression");
        }
    }

    //private List<ILogicalExpression> ExpandWildcard(Schema schema/*, ILogicalPlan plan*/)
    //{
    //    //var usingColumns = GetUsingColumns(plan);
    //    //var columnsToSkip = usingColumns.

    //    //if (!columnsToSkip.Any())
    //    //{
    //    return schema.Fields.Select(f => (ILogicalExpression)new Column(f.Name)).ToList();
    //    //}

    //    //var columns = schema.Fields.Select(f =>
    //    //{
    //    //    var col = new Column(f.Name);
    //    //    if (!columnsToSkip.Contains(col))
    //    //    {
    //    //        return (ILogicalExpression)col;
    //    //    }

    //    //    return null;
    //    //});

    //    //return columns.Where(c => c!= null).ToList()!;
    //}

    /// <summary>
    /// Relational expression from sql expression
    /// </summary>
    private ILogicalExpression SqlToExpr(Expression predicate, Schema schema)
    {
        var expr = SqlExprToLogicalExpr(predicate, schema);
        // rewrite qualifier
        // validate
        // infer

        return expr;
    }

    private ILogicalExpression SqlExprToLogicalExpr(Expression predicate, Schema schema)
    {
        if (predicate is Expression.BinaryOp b)
        {
            return ParseSqlBinaryOp(b.Left, b.Op, b.Right, schema);
        }

        return SqlExprToLogicalInternal(predicate, schema);
    }

    private ILogicalExpression ParseSqlBinaryOp(Expression left, BinaryOperator op, Expression right, Schema schema)
    {
        return new BinaryExpr(SqlExprToLogicalExpr(left, schema), op, SqlExprToLogicalExpr(right, schema));
    }

    private ILogicalExpression SqlExprToLogicalInternal(Expression expr, Schema schema)
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

    private ILogicalExpression SqlIdentifierToExpr(Expression.Identifier ident, Schema schema)
    {
        var field = schema.GetField(ident.Ident.Value);
        return new Column(field.Name);
    }

    private void ExprToColumns(ILogicalExpression expression, HashSet<Column> accumulator)
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

    private static void InspectExprPre(ILogicalExpression expression, Action<ILogicalExpression> action)
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

    private ILogicalExpression ParseValue(Expression.LiteralValue literalValue)
    {
        switch (literalValue.Value)
        {
            //TODO case Value.Null: literal scalar value

            case Value.Number n:
                return ParseSqlNumber(n);

            case Value.SingleQuotedString sq:
                return new LiteralExpression(sq.Value);

            case Value.Boolean b:
                return new LiteralExpression(b.Value.ToString());

            default:
                throw new NotImplementedException();
        }
    }

    private ILogicalExpression ParseSqlNumber(Value.Number number)
    {
        return new LiteralExpression(number.Value);
    }

    private ILogicalExpression SqlFunctionToExpr(Expression.Function function, Schema schema)
    {
        // scalar functions

        // aggregate functions
        var aggregate = AggregateFunction.FromString(function.Name);
        if (aggregate != null)
        {
            var (agFn, expressionArgs) = AggregateFunctionToExpr(aggregate, function.Args, schema);
            return new AggregateFunctionExpression(agFn, expressionArgs /*distinct*/);
        }

        return null;
    }

    private (AggregateFunction, List<ILogicalExpression>) AggregateFunctionToExpr(
        AggregateFunction function,
        Sequence<FunctionArg> args,
        Schema schema)
    {
        List<ILogicalExpression> arguments = null!;

        if (function is Count)
        {
            //
        }
        else
        {
            arguments = FunctionArgsToExpr(args, schema);
        }

        return (function, arguments);

    }

    private List<ILogicalExpression> FunctionArgsToExpr(Sequence<FunctionArg> args, Schema schema)
    {
        return args.Select(SqlFnArgToLogicalExpr).ToList();

        ILogicalExpression SqlFnArgToLogicalExpr(FunctionArg functionArg)
        {
            return functionArg switch
            {
                FunctionArg.Named {Arg: FunctionArgExpression.Wildcard} => new Wildcard(),
                FunctionArg.Named {Arg: FunctionArgExpression.FunctionExpression a} => SqlExprToLogicalExpr(a.Expression, schema),
                FunctionArg.Unnamed {FunctionArgExpression: FunctionArgExpression.FunctionExpression fe} => SqlExprToLogicalExpr(fe.Expression, schema),
                _ => throw new InvalidOleVariantTypeException($"Unsupported qualified wildcard argument: {functionArg.ToSql()}")
            };
        }
    }

    //private ILogicalExpression NormalizeColumn(ILogicalExpression expr, IEnumerable<Schema> schemas, HashSet<Column> usedColumns)
    //{
    //    return expr;
    //}
}

