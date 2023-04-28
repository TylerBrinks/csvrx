using CsvRx.Core.Logical;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;
using CsvRx.Core.Physical.Execution;

namespace CsvRx.Core.Physical;

internal class PhysicalPlanner
{
    public IExecutionPlan CreateInitialPlan(ILogicalPlan optimized)
    {
        switch (optimized)
        {
            case TableScan t:
                return t.Source.Scan(t.Projection);

            case Aggregate a:
                {
                    var inputExec = CreateInitialPlan(a.Plan);
                    var physicalSchema = inputExec.Schema;
                    var logicalSchema = a.Plan.Schema;

                    var groups = PhysicalExtensions.CreateGroupingPhysicalExpr(a.GroupExpressions, logicalSchema, physicalSchema);

                    var aggregates = a.AggregateExpressions
                        .Select(e => PhysicalExtensions.CreateAggregateExpression(e, logicalSchema, physicalSchema))
                        .ToList();

                    var initialAggregate = AggregateExecution.TryNew(AggregationMode.Partial, groups, aggregates, inputExec, physicalSchema);

                    var finalGroup = initialAggregate.OutputGroupExpr();

                    var finalGroupingSet = GroupBy.NewSingle(finalGroup.Select((e, i) => (e, groups.Expr[i].Name)).ToList());

                    return AggregateExecution.TryNew(AggregationMode.Final, finalGroupingSet, aggregates, initialAggregate, physicalSchema);
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
                                : LogicalExtensions.GetPhysicalName(e);
                        }
                        else
                        {
                            physicalName = LogicalExtensions.GetPhysicalName(e);
                        }

                        return (Expression: LogicalExtensions.CreatePhysicalExpr(e, inputSchema, inputExec.Schema), Name: physicalName);
                    }).ToList();

                    return ProjectionExecution.TryNew(physicalExpressions, inputExec);
                }
            case Filter f:
                {
                    var physicalInput = CreateInitialPlan(f.Plan);
                    var inputSchema = physicalInput.Schema;
                    var inputDfSchema = f.Plan.Schema;
                    var runtimeExpr = LogicalExtensions.CreatePhysicalExpr(f.Predicate, inputDfSchema, inputSchema);

                    return FilterExecution.TryNew(runtimeExpr, physicalInput);
                }

            case Distinct d:
            default:
                throw new NotImplementedException();
        }
    }
}