using CsvRx.Data;
using CsvRx.Logical;
using SqlParser.Ast;
//using CsvRx.Data;
//using CsvRx.Logical;
//using CsvRx.Physical;


var context = new ExecutionContext();
context.RegisterCsv("aggregate_test_100", @"C:\Users\tyler\source\repos\sink\sqldatafusion\testing\data\csv\aggregate_test_100.csv");
var df = context.Sql("SELECT c1, MAX(c12) FROM aggregate_test_100 GROUP BY c1"); //WHERE c11 > 0.1 AND c11 < 0.9 
//var df = context.Sql("SELECT MAX(c3) FROM aggregate_test_100");
//context.Execute(df);

Console.Write("done");

public class CsvOptions
{
    public string Delimiter { get; set; } = ",";
    public bool HasHeader { get; set; } = true;
    public int InferMax { get; set; } = 100;
}

public class ExecutionContext
{
    private readonly Dictionary<string, DataSource> _tables = new();

    public void RegisterCsv(string tableName, string path)
    {
        RegisterCsv(tableName, path, new CsvOptions());
    }

    public void RegisterCsv(string tableName, string path, CsvOptions options)
    {
        var csv = new CsvDataSource(path, options);
        //TODO: csv?
        Register(tableName, csv);
    }

    public void Register(string tableName, DataSource df)
    {
        _tables.Add(tableName, df);
    }

    public ILogicalPlan Sql(string sql)
    {
        var ast = new Parser().ParseSql(sql);

        if (ast.Count > 1)
        {
            throw new InvalidOperationException();
        }

        var plan = ast.First() switch
        {
            Statement.Select s => new Planner().CreateLogicalPlan(s.Query),
            _ => throw new InvalidOperationException()
        };

        return plan;
    }

    //public List<RecordBatch> Execute(DataFrame df)
    //{
    //    return Execute(df.LogicalPlan);
    //}

    //public List<RecordBatch> Execute(ILogicalPlan plan)
    //{
    //    var optimized = new Optimizer().Optimize(plan);
    //    var physical = new Planner().CreatePhysicalPlan(optimized);

    //    return physical.Execute();
    //}
}

public class Planner
{
    public ILogicalPlan CreateLogicalPlan(Query query)
    {
        var select = query.Body.AsSelect();

        var plan = PlanFromTables(select.From!);

        return plan;
    }

    private ILogicalPlan PlanFromTables(Sequence<TableWithJoins> from)
    {
        // check for cluster by, lateral views, quality, top, sort by
        //TODO multiple tables?

        return PlanTableWithJoins(from.First());
    }

    private ILogicalPlan PlanTableWithJoins(TableWithJoins table)
    {
       // todo if joins?

        return CreateRelation(table.Relation);
    }

    private ILogicalPlan CreateRelation(TableFactor table)
    {
        if (table is not TableFactor.Table relation)
        {
            throw new InvalidOperationException();
        }

        var name = relation.Alias != null ? relation.Alias.Name : relation.Name.Values[0];

        return new LogicalPlanBuilder().Scan().Build();
    }
}

public class LogicalPlanBuilder
{
    public LogicalPlanBuilder Scan(DataSource dataSource)
    {
        ScanWithFilters(dataSource);
        return this;
    }

    private void ScanWithFilters(DataSource dataSource)
    {
        var schema = dataSource.Schema;

    }

    public ILogicalPlan Build()
    {
        return null;
    }
}


//public class Optimizer
//{
//    public ILogicalPlan Optimize(ILogicalPlan plan)
//    {
//        return new ProjectionPushDownRule().Optimize(plan);
//    }
//}

//public class Planner
//{
//    public DataFrame CreateDataFrame(Query query, Dictionary<string, DataFrame> tables)
//    {
//        var select = query.Body.AsSelect();

//        var relation = select.From!.First().Relation!.AsTable();
//        var name = relation.Alias != null ? relation.Alias.Name : relation.Name.Values[0];
//        var table = tables[name];

//        var projectionExpr = select.Projection.Select(_ => CreateLogicalExpression(GetSelectExpression(_), table)).ToList();

//        var columnsInProjection = GetReferencedColumns(projectionExpr);
//        var aggregateExprCount = projectionExpr.Count(IsAggregateExpression);

//        //if (aggregateExprCount > 0 && select.GroupBy == null || !select.GroupBy!.Any())
//        if(aggregateExprCount == 0 && select.GroupBy != null && select.GroupBy.Any())
//        {
//            throw new InvalidOperationException();
//        }

//        var columnsInSelect = GetColumnsReferencedBySelection(select, table);

//        var plan = table;

//        if (aggregateExprCount == 0)
//        {
//            //return PlanNonAggregateQuery(select, table, projectionExpr, columnsInSelect, columnsInProjection);
//        }

//        var projection = new List<ILogicalExpression>();
//        var aggregates = new List<LogicalAggregateExpression>();
//        var groupColumnCount = select.GroupBy?.Count ?? 0;
//        var groupCount = 0;

//        foreach (var expr in projectionExpr)
//        {
//            if (expr is LogicalAggregateExpression ae)
//            {
//                projection.Add(new ColumnIndex(groupColumnCount + aggregates.Count));
//                aggregates.Add(ae);
//            }
//            //else if alias
//            else
//            {
//                projection.Add(new ColumnIndex(groupCount++));
//            }
//        }

//        plan = PlanAggregateQuery(projectionExpr, select, columnsInSelect, plan, aggregates);
//        plan = plan.Project(projection);

//        if (select.Having != null)
//        {
//            plan = plan.Filter(CreateLogicalExpression(select.Having, plan));
//        }

//        return plan;

//        Expression GetSelectExpression(SelectItem selectItem)
//        {
//            return selectItem switch
//            {
//                SelectItem.UnnamedExpression u => u.Expression,
//                SelectItem.ExpressionWithAlias e => e.Expression,
//                _ => throw new InvalidOperationException()
//            };
//        }
//    }

//    private DataFrame PlanAggregateQuery(
//        List<ILogicalExpression> projectionExpr,
//        Select select,
//        HashSet<string> columnsInSelect,
//        DataFrame plan,
//        List<LogicalAggregateExpression> aggregateExpr)
//    {
//        var projectionWithoutAggregates = projectionExpr.Where(_ => !IsAggregateExpression(_)).ToList();

//        if (select.Selection != null)
//        {
//            var columnNamesInProjectionWithoutAggregates = GetReferencedColumns(projectionWithoutAggregates);
//            var missing = columnsInSelect.Except(columnNamesInProjectionWithoutAggregates);

//            // if the selection only references outputs from the projection we can simply apply the filter
//            // expression to the DataFrame representing the projection
//            if (!missing.Any())
//            {
//                plan = plan.Project(projectionWithoutAggregates);
//                plan = plan.Filter(CreateLogicalExpression(select.Selection, plan));
//            }
//            else
//            {
//                // because the selection references some columns that are not in the projection output we
//                // need to create an interim projection that has the additional columns and then we need to
//                // remove them after the selection has been applied
//                var fullList = projectionWithoutAggregates.Concat(missing.Select(c => new Column(c))).ToList();
//                plan = plan.Project(fullList);
//                plan = plan.Filter(CreateLogicalExpression(select.Selection, plan));
//            }
//        }

//        var groupByExpr = select.GroupBy?.Select(_ => CreateLogicalExpression(_, plan)).ToList() ?? new List<ILogicalExpression>();
//        return plan.Aggregate(groupByExpr, aggregateExpr);
//    }

//    private HashSet<string> GetColumnsReferencedBySelection(Select select, DataFrame table)
//    {
//        var accumulator = new HashSet<string>();

//        if (select.Selection != null)
//        {
//            var filterExpr = CreateLogicalExpression(select.Selection, table);
//            Visit(filterExpr, accumulator);
//            var validColumns = table.Schema.Fields.Select(f => f.Name).ToList();

//            accumulator.RemoveWhere(_ => !validColumns.Contains(_));
//        }

//        return accumulator;
//    }

//    private HashSet<string> GetReferencedColumns(List<ILogicalExpression> projectionExpressions)
//    {
//        var accumulator = new HashSet<string>();

//        foreach (var expr in projectionExpressions)
//        {
//            Visit(expr, accumulator);
//        }

//        return accumulator;
//    }

//    private void Visit(ILogicalExpression expr, HashSet<string> accumulator)
//    {
//        switch (expr)
//        {
//            case Column c:
//                accumulator.Add(c.Name);
//                break;

//            //case Alias a ;

//            case BinaryExpr b:
//                Visit(b.Left, accumulator);
//                Visit(b.Right, accumulator);
//                break;

//            case LogicalAggregateExpression a:
//                Visit(a.Expression, accumulator);
//                break;
//        }
//    }

//    private static bool IsAggregateExpression(ILogicalExpression expr)
//    {
//        if (expr is LogicalAggregateExpression)
//        {
//            return true;
//        }

//        // if alias, return alias.expr is agg exp

//        return false;
//    }

//    //private ILogicalExpression CreateLogicalExpression(SelectItem selectItem, DataFrame table)
//    private static ILogicalExpression CreateLogicalExpression(Expression expr, DataFrame table)
//    {
//        return expr switch
//        {
//            Expression.Identifier i => new Column(i.Ident.Value),
//            Expression.LiteralValue v => GetLiteralValue(v.Value),
//            Expression.Function fn => GetFunction(fn),
//            Expression.BinaryOp b => GetBinaryExpr(b),
//            _ => throw new InvalidOperationException()
//        };

//        ILogicalExpression GetFunction(Expression.Function fn)
//        {
//            var arg = fn.Args?.FirstOrDefault();

//            var argVal = arg switch
//            {
//                FunctionArg.Unnamed u => u.FunctionArgExpression,
//                FunctionArg.Named n => n.Arg,
//                _ => throw new InvalidOperationException()
//            };

//            var argExp = argVal switch
//            {
//                FunctionArgExpression.FunctionExpression f => f.Expression,
//                _ => throw new InvalidOperationException()
//            };

//            return fn.Name.ToSql() switch
//            {
//                "MIN" => new MinFunction(CreateLogicalExpression(argExp, table)),
//                "MAX" => new MaxFunction(CreateLogicalExpression(argExp, table))
//            };
//        }

//        ILogicalExpression GetLiteralValue(Value value)
//        {
//            return value switch
//            {
//                Value.Number n => new NumberValue(n.Value),
//                _ => null
//            };
//        }

//        ILogicalExpression GetBinaryExpr(Expression.BinaryOp op)
//        {
//            var left = CreateLogicalExpression(op.Left, table);
//            var right = CreateLogicalExpression(op.Right, table);

//            return op.Op switch
//            {
//                BinaryOperator.Eq => new Eq(left, right),
//                BinaryOperator.NotEq => new Neq(left, right),
//                BinaryOperator.Gt => new Gt(left, right),
//                BinaryOperator.GtEq => new GtEq(left, right),
//                BinaryOperator.Lt => new Lt(left, right),
//                BinaryOperator.LtEq => new LtEq(left, right),
//                BinaryOperator.And => new And(left, right),
//                BinaryOperator.Or => new Or(left, right),
//                BinaryOperator.Plus => new Add(left, right),
//                BinaryOperator.Minus => new Subtract(left, right),
//                BinaryOperator.Multiply => new Multiply(left, right),
//                BinaryOperator.Divide => new Divide(left, right),
//                BinaryOperator.Modulo => new Modulus(left, right)
//            };
//        }
//    }

//    public IPhysicalPlan CreatePhysicalPlan(ILogicalPlan optimized)
//    {
//        switch (optimized)
//        {
//            case Scan scan:
//                return new ScanExec(scan.DataSource, scan.Projection);

//            case Selection s:
//                var inputS = CreatePhysicalPlan(s.Plan);
//                var filterExpr = CreatePhysicalExpr(s.Expr, s.Plan);
//                return new SelectionExec(inputS, filterExpr);

//            case Projection p:
//                var inputp = CreatePhysicalPlan(p.Plan);
//                var projectionExpr = p.Expr.Select(_ => CreatePhysicalExpr(_, p.Plan)).ToList();
//                var projectionSchema = new Schema(p.Expr.Select(_ => _.ToField(p.Plan)).ToList());
//                return new ProjectionExec(inputp, projectionSchema, projectionExpr);

//            case Aggregate a:
//                var inputA = CreatePhysicalPlan(a.Plan);
//                var groupExpr = a.Expr.Select(_ => CreatePhysicalExpr(_, a.Plan));
//                var aggregateExpr = a.AggregateExpr.Select(_ =>
//                {
//                    return _ switch
//                    {
//                        MaxFunction => (IPhysicalAggregateExpression)new MaxExpression(CreatePhysicalExpr(_.Expression, a.Plan)),
//                        MinFunction => new MinExpression(CreatePhysicalExpr(_.Expression, a.Plan)),
//                        _ => throw new InvalidOperationException()
//                    };
//                }).ToList();
//                return new HashAggregateExec(inputA, groupExpr, aggregateExpr, a.Schema);

//            default:
//                throw new InvalidOperationException();
//        }
//    }

//    private PhysicalExpression CreatePhysicalExpr(ILogicalExpression expr, ILogicalPlan plan)
//    {
//        switch (expr)
//        {
//            // todo Numerics here
//            case StringValue sv:
//                return new LiteralStringExpression(sv.Value);

//            default:
//                throw new InvalidOperationException();
//        }
//    }
//}

//public class ProjectionPushDownRule
//{
//    public ILogicalPlan Optimize(ILogicalPlan plan)
//    {
//        return PushDown(plan, new List<string>());
//    }

//    private ILogicalPlan PushDown(ILogicalPlan plan, List<string> columnNames)
//    {
//        switch (plan)
//        {
//            case Projection p:
//                ExtractColumns(p.Expr, p.Plan, columnNames);
//                var input = PushDown(p.Plan, columnNames);
//                return new Projection(input, p.Expr);

//            case Selection s:
//                ExtractColumns(s.Expr, s.Plan, columnNames);
//                var input2 = PushDown(s.Plan, columnNames);
//                return new Selection(input2, s.Expr);

//            case Aggregate a:
//                ExtractColumns(a.Expr, a.Plan, columnNames);
//                ExtractColumns(a.AggregateExpr.Select(a => a.Expression).ToList(), a.Plan, columnNames);
//                return new Aggregate(PushDown(a.Plan, columnNames), a.Expr, a.AggregateExpr);

//            case Scan scan:
//                var validFieldNames = scan.DataSource.Schema.Fields.Select(f => f.Name).ToList();
//                var pd = validFieldNames.Where(columnNames.Contains).OrderBy(f => f).ToList();
//                return new Scan(scan.Path, scan.DataSource, pd);

//            default:
//                throw new InvalidOperationException();
//        }
//    }

//    public void ExtractColumns(List<ILogicalExpression> expressions, ILogicalPlan input, List<string> accum)
//    {
//        foreach (var expr in expressions)
//        {
//            ExtractColumns(expr, input, accum);
//        }
//    }

//    public void ExtractColumns(ILogicalExpression expr, ILogicalPlan input, List<string> accum)
//    {
//        switch (expr)
//        {
//            case ColumnIndex ci:
//                accum.Add(input.Schema.Fields[ci.Index].Name);
//                break;

//            case Column c:
//                accum.Add(c.Name);
//                break;

//            case BinaryExpr b:
//                ExtractColumns(b.Left, input, accum);
//                ExtractColumns(b.Right, input, accum);
//                break;

//                //alias
//                // caset
//            case StringValue:
//            case NumberValue:
//                break;

//            default:
//                throw new InvalidOperationException();
//        }
//    }
//}

//public record RecordBatch(Schema Schema, List<ColumnVector> Fields)
//{
//    public int RowCount => Fields.First().Size;
//}

//public abstract record ColumnVector(int Size)
//{
//    public abstract object GetValue(int i);
//}


//public record LiteralValueVector(object Value, int Size) : ColumnVector(Size)
//{
//    public override object GetValue(int i)
//    {
//        if (i < 0 || i > Size)
//        {
//            throw new IndexOutOfRangeException();
//        }

//        return Value;
//    }
//}

//public record MinFunction(ILogicalExpression Expression) : LogicalAggregateExpression("MIN", Expression);

//public record MaxFunction(ILogicalExpression Expression) : LogicalAggregateExpression("MAX", Expression);

//public record StringValue(string Value) : ILogicalExpression
//{
//    public Field ToField(ILogicalPlan plan)
//    {
//        return new Field(Value);
//    }
//}

//public record NumberValue(string Value) : ILogicalExpression
//{
//    public Field ToField(ILogicalPlan plan)
//    {
//        return new Field(Value); //TODO data type
//    }
//}

//public record ColumnIndex(int Index) : ILogicalExpression
//{
//    public Field ToField(ILogicalPlan plan)
//    {
//        return plan.Schema.Fields[Index];
//    }
//}

//public interface IAccumulator
//{
//    void Accumulate(object value);
//    object FinalValue();
//}

//public interface IPhysicalAggregateExpression
//{
//    PhysicalExpression InputExpression { get; }
//    IAccumulator CreateAccumulator();
//}

//public record MaxExpression(PhysicalExpression Expr): IPhysicalAggregateExpression
//{
//    public PhysicalExpression InputExpression => Expr;
//    public IAccumulator CreateAccumulator()
//    {
//        throw new NotImplementedException();
//    }
//}

//public record MinExpression(PhysicalExpression Expr) : IPhysicalAggregateExpression
//{
//    public PhysicalExpression InputExpression => Expr;
//    public IAccumulator CreateAccumulator()
//    {
//        throw new NotImplementedException();
//    }
//}

