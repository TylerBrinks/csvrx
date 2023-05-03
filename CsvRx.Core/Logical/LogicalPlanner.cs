using CsvHelper.Configuration.Attributes;
using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;
using SqlParser.Ast;

namespace CsvRx.Core.Logical;

internal class LogicalPlanner
{
    public ILogicalPlan CreateLogicalPlan(Query query, Dictionary<string, DataSource> dataSources)
    {
        var select = query.Body.AsSelect();

        // Logical plans are rooted in scanning a table for values
        var plan = LogicalExtensions.PlanFromTable(select.From, dataSources);

        // Wrap the scan in a filter if a where clause exists
        plan = LogicalExtensions.PlanFromSelection(select.Selection, plan);

        // Build a select plan converting each select item into a logical expression
        var selectExpressions = LogicalExtensions.PrepareSelectExpressions(select.Projection, plan, plan is EmptyRelation);

        var projectedPlan = LogicalExtensions.PlanProjection(plan, selectExpressions);

        var combinedSchemas = projectedPlan.Schema;
        combinedSchemas.MergeSchemas(plan.Schema);

        var aggregateExpressions = LogicalExtensions.FindAggregateExprs(selectExpressions.Select(_ => _).ToList());
        var groupByExpressions = LogicalExtensions.FindGroupByExprs(select.GroupBy, combinedSchemas);// projectedPlan.Schema);

        var selectExpressionsPostAggregate = new List<ILogicalExpression>();
        var havingExprsionsPostAggregate = new List<ILogicalExpression>();

        if (groupByExpressions.Any() || aggregateExpressions.Any())
        {
            // Wrap the plan in an aggregation
            (plan, selectExpressionsPostAggregate, havingExprsionsPostAggregate) =
                LogicalExtensions.CreateAggregatePlan(plan, selectExpressions, null, groupByExpressions, aggregateExpressions);
        }
        else
        {
            //todo check having expr

            selectExpressionsPostAggregate = selectExpressions;
        }

        //having

        // Wrap the plan in a projection
        plan = LogicalExtensions.PlanProjection(plan, selectExpressionsPostAggregate);

        if (select.Distinct)
        {
            plan = new Distinct(plan);
        }

        // Wrap the plan in a sort
        plan = LogicalExtensions.OrderBy(plan, query.OrderBy);

        // Wrap the plan in a limit
        var final = LogicalExtensions.Limit(plan, query.Offset, query.Limit);

        return final;
    }
}