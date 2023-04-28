using CsvRx.Core.Data;
using CsvRx.Core.Logical;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Physical.Expressions;
using CsvRx.Core.Physical.Functions;

namespace CsvRx.Core.Physical
{
    internal static class PhysicalExtensions
    {
        internal static GroupBy CreateGroupingPhysicalExpr(
            List<ILogicalExpression> groupExpressions,
            Schema inputDfSchema,
            Schema inputSchema)
        {
            if (groupExpressions.Count == 1)
            {
                var expr = LogicalExtensions.CreatePhysicalExpr(groupExpressions[0], inputDfSchema, inputSchema);
                var name = LogicalExtensions.CreatePhysicalName(groupExpressions[0], true);

                var a = new List<(IPhysicalExpression Expression, string Name)>
                {
                    (expr, name)
                };

                return GroupBy.NewSingle(a);
            }

            var group = groupExpressions.Select(e =>
                (
                    LogicalExtensions.CreatePhysicalExpr(e, inputDfSchema, inputSchema),
                    LogicalExtensions.CreatePhysicalName(e, true)
                ))
                .ToList();

            return GroupBy.NewSingle(group);
        }

        internal static List<ColumnDataType> CoerceTypes(AggregateFunction fn, List<ColumnDataType> inputTypes)
        {
            switch (fn.FunctionType)
            {
                case AggregateFunctionType.Count:
                case AggregateFunctionType.ApproxDistinct:
                    return inputTypes;

                case AggregateFunctionType.ArrayAgg:
                case AggregateFunctionType.Min:
                case AggregateFunctionType.Max:
                    return inputTypes;

                case AggregateFunctionType.Sum:
                case AggregateFunctionType.Avg:
                    return inputTypes;

                default:
                    throw new NotImplementedException($"Function coercion not yet implemented for {fn.FunctionType}");
            }
        }

        internal static ColumnDataType GetReturnTypes(AggregateFunction fn, List<ColumnDataType> inputPhysicalTypes)
        {
            var coercedDataTypes = CoerceTypes(fn, inputPhysicalTypes);

            return fn.FunctionType switch
            {
                AggregateFunctionType.Count or AggregateFunctionType.ApproxDistinct => ColumnDataType.Integer,

                AggregateFunctionType.Min or AggregateFunctionType.Max => coercedDataTypes[0],

                AggregateFunctionType.Sum => SumReturnType(coercedDataTypes[0]),

                AggregateFunctionType.Avg => AverageReturnType(coercedDataTypes[0]),

                _ => throw new NotImplementedException()
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

        internal static Aggregate CreateAggregateExpr(
            AggregateFunction fn,
            bool distinct,
            List<IPhysicalExpression> inputPhysicalExpressions,
            Schema physicalSchema,
            string name)
        {
            var inputPhysicalTypes = inputPhysicalExpressions.Select(e => e.GetDataType(physicalSchema)).ToList();
            var returnType = GetReturnTypes(fn, inputPhysicalTypes);

            switch (fn.FunctionType, distinct)
            {
                case (AggregateFunctionType.Count, _):
                    return new CountFunction(inputPhysicalExpressions[0], name, returnType);

                case (AggregateFunctionType.Sum, _):
                    return new SumFunction(inputPhysicalExpressions[0], name, returnType);

                case (AggregateFunctionType.Min, _):
                    return new MinFunction(inputPhysicalExpressions[0], name, returnType);

                case (AggregateFunctionType.Max, _):
                    return new MaxFunction(inputPhysicalExpressions[0], name, returnType);

                default:
                    throw new NotImplementedException($"Aggregate function not yet implemented: {fn.FunctionType}");
            }
        }
        
        internal static Aggregate CreateAggregateExpression(ILogicalExpression expression, Schema logicalSchema, Schema physicalSchema)
        {
            //todo handle alias
            var name = LogicalExtensions.CreatePhysicalName(expression, true);

            return CreateAggregateExprWithName(expression, name, logicalSchema, physicalSchema);
        }

        internal static Aggregate CreateAggregateExprWithName(ILogicalExpression expression, string name, Schema logicalSchema, Schema physicalSchema)
        {
            switch (expression)
            {
                case AggregateFunction fn:
                    var args = fn.Args.Select(e => LogicalExtensions.CreatePhysicalExpr(e, logicalSchema, physicalSchema)).ToList();
                    return CreateAggregateExpr(fn, fn.Distinct, args, physicalSchema, name);

                default:
                    throw new NotImplementedException("Aggregate function not implemented");
            }
        }
    }
}