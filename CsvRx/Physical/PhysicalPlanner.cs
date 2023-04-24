using CsvRx.Data;
using CsvRx.Logical;
using CsvRx.Logical.Expressions;
using CsvRx.Logical.Functions;
using CsvRx.Logical.Plans;
using CsvRx.Physical.Execution;
using CsvRx.Physical.Expressions;

namespace CsvRx.Physical
{
    public class PhysicalPlanner
    {
        public IExecutionPlan CreateInitialPlan(ILogicalPlan optimized)
        {
            switch (optimized)
            {
                case TableScan t:
                    {
                        return t.Source.Scan(t.Projection);
                    }

                case Aggregate a:
                    {
                        var inputExec = CreateInitialPlan(a.Plan);
                        var physicalSchema = inputExec.Schema;
                        var logicalSchema = a.Plan.Schema;

                        var groups = CreateGroupingPhysicalExpr(a.GroupExpressions, logicalSchema, physicalSchema);

                        var aggregates = a.AggregateExpressions
                            .Select(e => CreateAggregateExpression(e, logicalSchema, physicalSchema))
                            .ToList();

                        var initialAggregate = AggregateExec.TryNew(AggregationMode.Partial, groups, aggregates, inputExec, physicalSchema);

                        var finalGroup = initialAggregate.OutputGroupExpr();

                        var finalGroupingSet = PhysicalGroupBy.NewSingle(finalGroup.Select((e, i) => (e, groups.Expr[i].Name)).ToList());

                        return AggregateExec.TryNew(AggregationMode.Final, finalGroupingSet, aggregates, initialAggregate, physicalSchema);
                    }
                case Projection p:
                    {
                        var inputExec = CreateInitialPlan(p.Plan);
                        var inputSchema = p.Plan.Schema;

                        var physicalExpressions = p.Expr.Select(e =>
                        {
                            string physicalName;

                            if (e is Column col)
                            {
                                var index = inputSchema.IndexOfColumn(col);
                                physicalName = index != null
                                    ? inputExec.Schema.Fields[index.Value].Name
                                    : Extensions.GetPhysicalName(e);
                            }
                            else
                            {
                                physicalName = Extensions.GetPhysicalName(e);
                            }

                            return (Expression: Extensions.CreatePhysicalExpr(e, inputSchema, inputExec.Schema), Name: physicalName);
                        }).ToList();

                        return ProjectionExec.TryNew(physicalExpressions, inputExec);
                    }
                case Filter f:
                    {
                        var physicalInput = CreateInitialPlan(f.Plan);
                        var inputSchema = physicalInput.Schema;
                        var inputDfSchema = f.Plan.Schema;
                        var runtimeExpr = Extensions.CreatePhysicalExpr(f.Predicate, inputDfSchema, inputSchema);

                        return FilterExec.TryNew(runtimeExpr, physicalInput);
                    }
                case Distinct d:
                default:
                    throw new NotImplementedException();
            };
        }

        private AggregateExpression CreateAggregateExpression(ILogicalExpression expression, Schema logicalSchema, Schema physicalSchema)
        {
            //todo handle alias
            var name = Extensions.CreatePhysicalName(expression, true);

            return CreateAggregateExprWithName(expression, name, logicalSchema, physicalSchema);
        }

        private AggregateExpression CreateAggregateExprWithName(ILogicalExpression expression, string name, Schema logicalSchema, Schema physicalSchema)
        {
            switch (expression)
            {
                case AggregateFunction fn:
                    var args = fn.Args.Select(e => Extensions.CreatePhysicalExpr(e, logicalSchema, physicalSchema)).ToList();
                    return CreateAggregateExpr(fn, fn.Distinct, args, physicalSchema, name);

                default:
                    throw new NotImplementedException("Aggregate function not implemented");
            }
        }

        private AggregateExpression CreateAggregateExpr(
            AggregateFunction fn,
            bool distinct,
            List<IPhysicalExpression> inputPhysicalExprs,
            Schema physicalSchema,
            string name)
        {
            var inputPhysicalTypes = inputPhysicalExprs.Select(e => e.GetDataType(physicalSchema)).ToList();
            var returnType = ReturnTypes(fn, inputPhysicalTypes);

            switch (fn.FunctionType, distinct)
            {
                case (AggregateFunctionType.Count, _):
                    return new CountFunction(inputPhysicalExprs[0], name, returnType);

                case (AggregateFunctionType.Sum, _):
                    return new SumFunction(inputPhysicalExprs[0], name, returnType);

                case (AggregateFunctionType.Min, _):
                    return new MinFunction(inputPhysicalExprs[0], name, returnType);

                case (AggregateFunctionType.Max, _):
                    return new MaxFunction(inputPhysicalExprs[0], name, returnType);

                default:
                    throw new NotImplementedException($"Aggregate function not yet implemented: {fn.FunctionType}");
            }
        }

        private ColumnDataType ReturnTypes(AggregateFunction fn, List<ColumnDataType> inputPhysicalTypes)
        {
            var coercedDataTypes = CoerceTypes(fn, inputPhysicalTypes);

            return fn.FunctionType switch
            {
                AggregateFunctionType.Count or AggregateFunctionType.ApproxDistinct => ColumnDataType.Integer,

                AggregateFunctionType.Min or AggregateFunctionType.Max => coercedDataTypes[0],

                AggregateFunctionType.Sum => SumReturnType(coercedDataTypes[0]),

                AggregateFunctionType.Avg => AverageReturnType(coercedDataTypes[0]),

            };

            ColumnDataType SumReturnType(ColumnDataType dataType)
            {
                return dataType switch
                {
                    ColumnDataType.Integer => ColumnDataType.Integer,
                    _ => throw new InvalidOperationException($"SUM does not support data type {dataType}")
                };
            }

            ColumnDataType AverageReturnType(ColumnDataType dataType)
            {
                return dataType switch
                {
                    ColumnDataType.Integer or ColumnDataType.Decimal => ColumnDataType.Decimal,
                    _ => throw new InvalidOperationException($"AVG does not support data type {dataType}")
                };
            }
        }

        private List<ColumnDataType> CoerceTypes(AggregateFunction fn, List<ColumnDataType> inputTypes)
        {
            switch (fn.FunctionType)
            {
                case AggregateFunctionType.Count:
                case AggregateFunctionType.ApproxDistinct:
                    return inputTypes;

                case AggregateFunctionType.ArrayAgg:
                case AggregateFunctionType.Min:
                case AggregateFunctionType.Max:
                    return inputTypes;// GetMinMaxResultType(inputTypes);

                case AggregateFunctionType.Sum:
                case AggregateFunctionType.Avg:
                    return inputTypes;

                default:
                    throw new NotImplementedException($"Function coercion not yet implemented for {fn.FunctionType}");
            }
        }

        private PhysicalGroupBy CreateGroupingPhysicalExpr(List<ILogicalExpression> groupExpressions, Schema inputDfSchema, Schema inputSchema)
        {
            if (groupExpressions.Count == 1)
            {
                var expr = Extensions.CreatePhysicalExpr(groupExpressions[0], inputDfSchema, inputSchema);
                var name = Extensions.CreatePhysicalName(groupExpressions[0], true);

                var a = new List<(IPhysicalExpression Expression, string Name)>
                {
                    (expr, name)
                };

                return PhysicalGroupBy.NewSingle(a);
            }

            var group = groupExpressions.Select(e =>
                (
                    Extensions.CreatePhysicalExpr(e, inputDfSchema, inputSchema),
                    Extensions.CreatePhysicalName(e, true)
                ))
                .ToList();

            return PhysicalGroupBy.NewSingle(group);
        }
    }

    public record PhysicalGroupBy(
        List<(IPhysicalExpression Expression, string Name)> Expr,
        List<(IPhysicalExpression Expression, string Name)> NullExpressions,
        List<List<bool>> Groups)
    {
        public static PhysicalGroupBy NewSingle(List<(IPhysicalExpression Expression, string Name)> expressions)
        {
            return new PhysicalGroupBy(
                expressions,
                new List<(IPhysicalExpression Expression, string Name)>(),
                new List<List<bool>>(expressions.Count));
        }
    }
}
