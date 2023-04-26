using System.Runtime.InteropServices;
using CsvRx.Data;
using CsvRx.Logical.Expressions;
using CsvRx.Logical.Functions;
using CsvRx.Logical.Plans;
using SqlParser.Ast;

namespace CsvRx.Logical;

public class LogicalPlanner
{
    public ILogicalPlan CreateLogicalPlan(Query query, Dictionary<string, DataSource> dataSources)
    {
        var select = query.Body.AsSelect();

        // Logical plans are rooted in scanning a table for values
        var plan = Extensions.PlanFromTable(select.From, dataSources);

        // Wrap the scan in a filter if a where clause exists
        plan = Extensions.PlanFromSelection(select.Selection, plan);

        // Build a select plan converting each select item into a logical expression
        var selectExpressions = Extensions.PrepareSelectExpressions(select.Projection, plan, plan is EmptyRelation);

        var projectedPlan = Extensions.PlanProjection(plan, selectExpressions);

        var aggregateExpressions = Extensions.FindAggregateExprs(selectExpressions.Select(_ => _).ToList());
        var groupByExpressions = Extensions.FindGroupByExprs(select.GroupBy, projectedPlan.Schema);

        var selectExpressionsPostAggregate = new List<ILogicalExpression>();
        var havingExprsionsPostAggregate = new List<ILogicalExpression>();

        if (groupByExpressions.Any() || aggregateExpressions.Any())
        {
            // Wrap the plan in an aggregation
            (plan, selectExpressionsPostAggregate, havingExprsionsPostAggregate) =
                Extensions.CreateAggregatePlan(plan, selectExpressions, null, groupByExpressions, aggregateExpressions);
        }
        else
        {
            //todo check having expr
        }

        //having

        // Wrap the plan in a projection
        plan = Extensions.PlanProjection(plan, selectExpressionsPostAggregate);

        if (select.Distinct)
        {
            plan = new Distinct(plan);
        }

        // Wrap the plan in a sort
        plan = Extensions.OrderBy(plan, query.OrderBy);

        // Wrap the plan in a limit
        var final = Extensions.Limit(plan, query.Offset, query.Limit);

        return final;
    }
}