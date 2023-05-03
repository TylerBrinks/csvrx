﻿using CsvRx.Core.Data;
using CsvRx.Core.Execution;
using CsvRx.Core.Logical;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;
using CsvRx.Core.Physical.Expressions;
using Aggregate = CsvRx.Core.Logical.Plans.Aggregate;
using Column = CsvRx.Core.Logical.Expressions.Column;

namespace CsvRx.Core.Physical;

internal class PhysicalPlanner
{
    public IExecutionPlan CreateInitialPlan(ILogicalPlan optimized)
    {
        return optimized switch
        {
            TableScan table => table.Source.Scan(table.Projection),
            Aggregate aggregate => CreateAggregatePlan(aggregate),
            Projection projection => CreateProjectionPlan(projection),
            Filter filter => CreateFilterPlan(filter),
            Sort sort => CreateSortPlan(sort),
           
            // Distinct should have been replaced by an 
            // aggregate plan by this point.
            Distinct => throw new NotImplementedException(),
            _ => throw new NotImplementedException()
        };
    }

    private IExecutionPlan CreateProjectionPlan(Projection projection)
    {
        var inputExec = CreateInitialPlan(projection.Plan);
        var inputSchema = projection.Plan.Schema;

        var physicalExpressions = projection.Expr.Select(e =>
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

    private IExecutionPlan CreateAggregatePlan(Aggregate aggregate)
    {
        var inputExec = CreateInitialPlan(aggregate.Plan);
        var physicalSchema = inputExec.Schema;
        var logicalSchema = aggregate.Plan.Schema;

        var groups = PhysicalExtensions.CreateGroupingPhysicalExpr(aggregate.GroupExpressions, logicalSchema, physicalSchema);

        var aggregates = aggregate.AggregateExpressions
            .Select(e => PhysicalExtensions.CreateAggregateExpression(e, logicalSchema, physicalSchema))
            .ToList();

        var initialAggregate =
            AggregateExecution.TryNew(AggregationMode.Partial, groups, aggregates, inputExec, physicalSchema);

        var finalGroup = initialAggregate.OutputGroupExpr();

        var finalGroupingSet = GroupBy.NewSingle(finalGroup.Select((e, i) => (e, groups.Expr[i].Name)).ToList());

        return AggregateExecution.TryNew(AggregationMode.Final, finalGroupingSet, aggregates, initialAggregate,
            physicalSchema);
    }
   
    private IExecutionPlan CreateFilterPlan(Filter filter)
    {
        var physicalInput = CreateInitialPlan(filter.Plan);
        var inputSchema = physicalInput.Schema;
        var inputDfSchema = filter.Plan.Schema;
        var runtimeExpr = LogicalExtensions.CreatePhysicalExpr(filter.Predicate, inputDfSchema, inputSchema);

        return FilterExecution.TryNew(runtimeExpr, physicalInput);
    }

    private IExecutionPlan CreateSortPlan(Sort sort)
    {
        var physicalInput = CreateInitialPlan(sort.Plan);
        var inputSchema = physicalInput.Schema;
        var sortSchema = sort.Plan.Schema;

        var sortExpressions = sort.OrderByExpressions
            .Select(e =>
            {
                if (e is OrderBy o)
                {
                    return CreatePhysicalSortExpression(o.Expression, sortSchema, inputSchema);
                }

                throw new InvalidOperationException("Sort only accepts sort expressions");
            }).ToList();

        return SortExecution.TryNew(sortExpressions, physicalInput);
    }

    private PhysicalSortExpression CreatePhysicalSortExpression(ILogicalExpression expression, Schema sortSchema, Schema inputSchema)
    {
        var physicalExpression = LogicalExtensions.CreatePhysicalExpr(expression, sortSchema, inputSchema);
        return new PhysicalSortExpression(physicalExpression, sortSchema, inputSchema);
    }
}