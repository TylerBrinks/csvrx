using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;
using SqlParser.Ast;

namespace CsvRx.Core.Logical;

internal class LogicalPlanner
{
    public static ILogicalPlan CreateLogicalPlan(Query query, Dictionary<string, DataSource> dataSources)
    {
        var select = query.Body.AsSelect();

        var tableReferences = select.CreateTableRelations();
        var context = new PlannerContext(dataSources, tableReferences);

        // Logical plans are rooted in scanning a table for values
        var plan = select.From.PlanTableWithJoins(context);

        // Wrap the scan in a filter if a where clause exists
        plan = select.Selection.PlanFromSelection(plan);

        // Build a select plan converting each select item into a logical expression
        var selectExpressions = select.Projection.PrepareSelectExpressions(plan, plan is EmptyRelation);

        var projectedPlan = plan.PlanProjection(selectExpressions);

        var combinedSchemas = projectedPlan.Schema;
        combinedSchemas.MergeSchemas(plan.Schema);

        var aliasMap = selectExpressions.Where(e => e is Alias).Cast<Alias>().ToDictionary(a => a.Name, a => a.Expression);

        var havingExpression = select.Having.MapHaving(combinedSchemas, aliasMap);

        var aggregateExpressionList = selectExpressions.ToList();
        if (havingExpression != null)
        {
            aggregateExpressionList.Add(havingExpression);
        }

        //TODO 2nd ToList necessary?
        var aggregateExpressions = aggregateExpressionList.ToList().FindAggregateExpressions();

        // check group by expressions inside FindGroupByExpressions, select.rs.line 130
        var groupByExpressions = select.GroupBy.FindGroupByExpressions(
            selectExpressions,
            combinedSchemas,
            projectedPlan, //plan
            aliasMap);

        List<ILogicalExpression>? selectPostAggregate;
        ILogicalExpression? havingPostAggregate;

        if (groupByExpressions.Any() || aggregateExpressions.Any())
        {
            // Wrap the plan in an aggregation
            (plan, selectPostAggregate, havingPostAggregate) = plan.CreateAggregatePlan(
                selectExpressions, havingExpression, groupByExpressions, aggregateExpressions);
        }
        else
        {
            selectPostAggregate = selectExpressions;
            if (havingExpression != null)
            {
                throw new InvalidOperationException("HAVING clause must appear in the GROUP BY clause or be used in an aggregate function.");
            }

            havingPostAggregate = null;
        }

        if (havingPostAggregate != null)
        {
            plan = new Filter(plan, havingPostAggregate);
        }

        // Wrap the plan in a projection
        plan = plan.PlanProjection(selectPostAggregate);

        if (select.Distinct)
        {
            plan = new Distinct(plan);
        }

        // Wrap the plan in a sort
        plan = plan.OrderBy(query.OrderBy);

        // Wrap the plan in a limit
        return plan.Limit(query.Offset, query.Limit);
    }
}