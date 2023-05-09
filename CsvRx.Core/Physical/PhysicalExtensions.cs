using CsvRx.Core.Data;
using CsvRx.Core.Logical;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Physical.Expressions;
using CsvRx.Core.Physical.Functions;

namespace CsvRx.Core.Physical
{
    internal static class PhysicalExtensions
    {
        internal static GroupBy CreateGroupingPhysicalExpression(
            List<ILogicalExpression> groupExpressions,
            Schema inputDfSchema,
            Schema inputSchema)
        {
            if (groupExpressions.Count == 1)
            {
                var expr = LogicalExtensions.CreatePhysicalExpression(groupExpressions[0], inputDfSchema, inputSchema);
                var name = LogicalExtensions.CreatePhysicalName(groupExpressions[0], true);

                var a = new List<(IPhysicalExpression Expression, string Name)>
                {
                    (expr, name)
                };

                return GroupBy.NewSingle(a);
            }

            var group = groupExpressions.Select(e =>
                (
                    LogicalExtensions.CreatePhysicalExpression(e, inputDfSchema, inputSchema),
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
                    //return inputTypes;

                case AggregateFunctionType.ArrayAgg:
                case AggregateFunctionType.Min:
                case AggregateFunctionType.Max:
                    //return inputTypes;

                case AggregateFunctionType.Sum:
                case AggregateFunctionType.Avg:
                case AggregateFunctionType.Median:
                case AggregateFunctionType.StdDev:
                case AggregateFunctionType.StdDevPop:
                case AggregateFunctionType.Variance:
                case AggregateFunctionType.VariancePop:
                case AggregateFunctionType.Covariance:
                case AggregateFunctionType.CovariancePop:
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

                AggregateFunctionType.Avg => NumericReturnType("AVG", coercedDataTypes[0]),

                AggregateFunctionType.Median
                    or AggregateFunctionType.StdDev
                    or AggregateFunctionType.StdDevPop 
                    or AggregateFunctionType.Variance
                    or AggregateFunctionType.VariancePop
                    or AggregateFunctionType.Covariance
                    or AggregateFunctionType.CovariancePop
                    => NumericReturnType(fn.FunctionType.ToString(), coercedDataTypes[0]),
            

                _ => throw new NotImplementedException("GetReturnTypes not implemented")
            };

            ColumnDataType SumReturnType(ColumnDataType dataType)
            {
                return dataType switch
                {
                    ColumnDataType.Integer => ColumnDataType.Integer,
                    ColumnDataType.Double => ColumnDataType.Double,
                    _ => throw new InvalidOperationException($"SUM does not support data type {dataType}")
                };
            }

            ColumnDataType NumericReturnType(string functionName, ColumnDataType dataType)
            {
                return dataType switch
                {
                    ColumnDataType.Integer or ColumnDataType.Double => ColumnDataType.Double,
                    _ => throw new InvalidOperationException($"{functionName} does not support data type {dataType}")
                };
            }
        }

        internal static Aggregate CreateAggregateExpression(
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

                case (AggregateFunctionType.Avg, _):
                    return new AverageFunction(inputPhysicalExpressions[0], name, returnType);

                case (AggregateFunctionType.Median, _):
                    return new MedianFunction(inputPhysicalExpressions[0], name, returnType);

                case (AggregateFunctionType.StdDev, _):
                    return new StandardDeviationFunction(inputPhysicalExpressions[0], name, returnType, StatisticType.Sample);

                case (AggregateFunctionType.StdDevPop, _):
                    return new StandardDeviationFunction(inputPhysicalExpressions[0], name, returnType, StatisticType.Population);

                case (AggregateFunctionType.Variance, _):
                    return new VarianceFunction(inputPhysicalExpressions[0], name, returnType, StatisticType.Sample);

                case (AggregateFunctionType.VariancePop, _):
                    return new VarianceFunction(inputPhysicalExpressions[0], name, returnType, StatisticType.Population);

                case (AggregateFunctionType.Covariance, _):
                    return new CovarianceFunction(inputPhysicalExpressions[0], inputPhysicalExpressions[1],
                        name, returnType, StatisticType.Sample);

                case (AggregateFunctionType.CovariancePop, _):
                    return new CovarianceFunction(inputPhysicalExpressions[0], inputPhysicalExpressions[1],
                        name, returnType, StatisticType.Population);

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
                    var args = fn.Args.Select(e => LogicalExtensions.CreatePhysicalExpression(e, logicalSchema, physicalSchema)).ToList();
                    return CreateAggregateExpression(fn, fn.Distinct, args, physicalSchema, name);

                default:
                    throw new NotImplementedException("Aggregate function not implemented");
            }
        }

        internal static PhysicalSortExpression CreatePhysicalSortExpression(
            ILogicalExpression expression,
            Schema sortSchema,
            Schema inputSchema,
            bool ascending)
        {
            var physicalExpression = LogicalExtensions.CreatePhysicalExpression(expression, sortSchema, inputSchema);
            return new PhysicalSortExpression(physicalExpression, sortSchema, inputSchema, ascending);
        }
    }
}