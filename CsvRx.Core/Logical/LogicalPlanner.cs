using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;
using SqlParser.Ast;

namespace CsvRx.Core.Logical;

public record TableReference(string Name, string? Alias = null);

internal class LogicalPlanner
{
    private List<TableReference> _tableReferences;

    public ILogicalPlan CreateLogicalPlan(Query query, Dictionary<string, DataSource> dataSources)
    {
        var select = query.Body.AsSelect();

        CreateTableRelations(select);

        // Logical plans are rooted in scanning a table for values
        var plan = LogicalExtensions.PlanFromTable(select.From, dataSources);

        // Wrap the scan in a filter if a where clause exists
        plan = LogicalExtensions.PlanFromSelection(select.Selection, plan);

        // Build a select plan converting each select item into a logical expression
        var selectExpressions = LogicalExtensions.PrepareSelectExpressions(select.Projection, plan, plan is EmptyRelation);

        var projectedPlan = LogicalExtensions.PlanProjection(plan, selectExpressions);

        var combinedSchemas = projectedPlan.Schema;
        combinedSchemas.MergeSchemas(plan.Schema);

        var aliasMap = selectExpressions.Where(e => e is Alias).Cast<Alias>().ToDictionary(a => a.Name, a => a.Expression);

        var havingExpression = LogicalExtensions.MapHaving(select.Having, combinedSchemas, aliasMap);

        var aggregateHaystack = selectExpressions.ToList();
        if (havingExpression != null)
        {
            aggregateHaystack.Add(havingExpression);
        }

        var aggregateExpressions = LogicalExtensions.FindAggregateExpressions(aggregateHaystack.ToList());

        // check group by expressions inside FindGroupByExpressions, select.rs.line 130
        var groupByExpressions = LogicalExtensions.FindGroupByExpressions(
            select.GroupBy,
            selectExpressions,
            combinedSchemas,
            plan,
            aliasMap);

        List<ILogicalExpression>? selectPostAggregate;
        ILogicalExpression? havingPostAggregate;

        if (groupByExpressions.Any() || aggregateExpressions.Any())
        {
            // Wrap the plan in an aggregation
            (plan, selectPostAggregate, havingPostAggregate) =
                LogicalExtensions.CreateAggregatePlan(plan, selectExpressions, havingExpression, groupByExpressions, aggregateExpressions);
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
        plan = LogicalExtensions.PlanProjection(plan, selectPostAggregate);

        if (select.Distinct)
        {
            plan = new Distinct(plan);
        }

        // Wrap the plan in a sort
        plan = LogicalExtensions.OrderBy(plan, query.OrderBy);

        // Wrap the plan in a limit
        return LogicalExtensions.Limit(plan, query.Offset, query.Limit);
    }

    private void CreateTableRelations(Select select)
    {
        var relationVisitor = new RelationVisitor();
        ((IElement)select).Visit(relationVisitor);
        _tableReferences = relationVisitor.TableReferences;
    }
}