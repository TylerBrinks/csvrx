using CsvRx.Core.Data;
using CsvRx.Core.Execution;
using CsvRx.Core.Logical;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;
using CsvRx.Core.Physical.Joins;
using Aggregate = CsvRx.Core.Logical.Plans.Aggregate;
using Column = CsvRx.Core.Logical.Expressions.Column;

namespace CsvRx.Core.Physical;

internal class PhysicalPlanner
{
    public IExecutionPlan CreateInitialPlan(ILogicalPlan optimized)
    {
        return optimized switch
        {
            TableScan table => table.Source.Scan(table.Projection!),
            Aggregate aggregate => CreateAggregatePlan(aggregate),
            Projection projection => CreateProjectionPlan(projection),
            Filter filter => CreateFilterPlan(filter),
            Sort sort => CreateSortPlan(sort),
            Limit limit => CreateLimitPlan(limit),
            SubqueryAlias alias => CreateInitialPlan(alias.Plan),
            Join join => CreateJoinPlan(join),

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

        var physicalExpressions = projection.Expression.Select(e =>
        {
            string physicalName;

            if (e is Column col)
            {
                var index = inputSchema.IndexOfColumn(col);
                physicalName = index != null
                    ? inputExec.Schema.Fields[index.Value].Name
                    : e.GetPhysicalName();
            }
            else
            {
                physicalName = e.GetPhysicalName();
            }

            return (Expression: e.CreatePhysicalExpression(inputSchema, inputExec.Schema), Name: physicalName);
        }).ToList();

        return ProjectionExecution.TryNew(physicalExpressions, inputExec);
    }

    private IExecutionPlan CreateAggregatePlan(Aggregate aggregate)
    {
        var inputExec = CreateInitialPlan(aggregate.Plan);
        var physicalSchema = inputExec.Schema;
        var logicalSchema = aggregate.Plan.Schema;

        var groups = aggregate.GroupExpressions.CreateGroupingPhysicalExpression(logicalSchema, physicalSchema);

        var aggregates = aggregate.AggregateExpressions
            .Select(e => e.CreateAggregateExpression(logicalSchema, physicalSchema))
            .ToList();

        var initialAggregate = AggregateExecution.TryNew(AggregationMode.Partial, groups, aggregates, inputExec, physicalSchema);

        var finalGroup = initialAggregate.OutputGroupExpression();

        var finalGroupingSet = GroupBy.NewSingle(finalGroup.Select((e, i) => (e, groups.Expression[i].Name)).ToList());

        return AggregateExecution.TryNew(AggregationMode.Final, finalGroupingSet, aggregates, initialAggregate, physicalSchema);
    }

    private IExecutionPlan CreateFilterPlan(Filter filter)
    {
        var physicalInput = CreateInitialPlan(filter.Plan);
        var inputSchema = physicalInput.Schema;
        var inputDfSchema = filter.Plan.Schema;
        var runtimeExpr = filter.Predicate.CreatePhysicalExpression(inputDfSchema, inputSchema);

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
                if (e is OrderBy order)
                {
                    return order.Expression.CreatePhysicalSortExpression(sortSchema, inputSchema, order.Ascending);
                }

                throw new InvalidOperationException("Sort only accepts sort expressions");
            }).ToList();

        return new SortExecution(sortExpressions, physicalInput);
    }

    private IExecutionPlan CreateLimitPlan(Limit limit)
    {
        var physicalInput = CreateInitialPlan(limit.Plan);

        var skip = limit.Skip ?? 0;
        var fetch = limit.Fetch ?? int.MaxValue;

        return new LimitExecution(physicalInput, skip, fetch);
    }

    private IExecutionPlan CreateJoinPlan(Join join)
    {
        var leftSchema = join.Plan.Schema;
        var leftPlan = CreateInitialPlan(join.Plan);

        var rightSchema = join.Right.Schema;
        var rightPlan = CreateInitialPlan(join.Right);

        var joinOn = join.On.Select(k =>
        {
            var leftColumn = (Column)k.Left;
            var rightColumn = (Column)k.Right;

            return new JoinOn(
                new Expressions.Column(leftColumn.Name, leftSchema.IndexOfColumn(leftColumn)!.Value),
                new Expressions.Column(rightColumn.Name, rightSchema.IndexOfColumn(rightColumn)!.Value)
            );
        }).ToList();

        var joinFilter = CreateJoinFilter();

        var (schema, columnIndices) = BuildJoinSchema(leftPlan.Schema, rightPlan.Schema, join.JoinType);

        if (!joinOn.Any())
        {
            return new NestedLoopJoinExecution(leftPlan, rightPlan, joinFilter, join.JoinType, columnIndices, schema);
        }

        return new HashJoinExecution(leftPlan, rightPlan, joinOn, joinFilter,
            join.JoinType, PartitionMode.CollectLeft, columnIndices, false, schema);

        JoinFilter? CreateJoinFilter()
        {
            if (join.Filter == null)
            {
                return null;
            }

            var columns = new HashSet<Column>();
            join.Filter.ExpressionToColumns(columns);

            // Collect left & right field indices, the field indices are sorted in ascending order
            var leftFieldIndices = columns.Select(leftSchema.IndexOfColumn) //.IndexOfQualifiedColumn)
                .Where(i => i != null)
                .Select(i => i!.Value)
                .OrderBy(i => i)
                .ToList();

            var rightFieldIndices = columns.Select(rightSchema.IndexOfColumn) //.IndexOfQualifiedColumn)
                .Where(i => i != null)
                .Select(i => i!.Value)
                .OrderBy(i => i)
                .ToList();

            var leftFilterFields = leftFieldIndices
                .Select(i => (leftSchema.Fields[i], leftPlan.Schema.Fields[i]));

            var rightFilterFields = rightFieldIndices
                .Select(i => (rightSchema.Fields[i], rightPlan.Schema.Fields[i]));

            var filterFields = leftFilterFields.Concat(rightFilterFields).ToList();

            // Construct intermediate schemas used for filtering data and
            // convert logical expression to physical according to filter schema
            var filterDfSchema = new Schema(filterFields.Select(f => f.Item1).ToList());
            var filterSchema = new Schema(filterFields.Select(f => f.Item2).ToList());

            var filterExpression = join.Filter!.CreatePhysicalExpression(filterDfSchema, filterSchema);

            var leftIndices = leftFieldIndices.Select(i => new ColumnIndex(i, JoinSide.Left));
            var rightIndices = rightFieldIndices.Select(i => new ColumnIndex(i, JoinSide.Right));

            var allIndices = leftIndices.Concat(rightIndices).ToList();

            return new JoinFilter(filterExpression, allIndices, filterSchema);
        }
    }

    private static (Schema, List<ColumnIndex>) BuildJoinSchema(Schema left, Schema right, JoinType joinType)
    {
        List<QualifiedField> fields;
        List<ColumnIndex> columnIndices;

        switch (joinType)
        {
            case JoinType.Inner:
            case JoinType.Left:
            case JoinType.Full:
            case JoinType.Right:
            {
                var leftFields = left.Fields
                    .Select(f => OutputJoinField(f, joinType, true))
                    .Select((f, i) => (Field: f, ColumnIndex: new ColumnIndex(i, JoinSide.Left)))
                    .ToList();

                var rightFields = right.Fields
                    .Select(f => OutputJoinField(f, joinType, false))
                    .Select((f, i) => (Field: f, ColumnIndex: new ColumnIndex(i, JoinSide.Right)))
                    .ToList();

                fields = leftFields
                    .Select(f => f.Field).Concat(rightFields.Select(f => f.Field))
                    .ToList();

                columnIndices = leftFields
                    .Select(f => f.ColumnIndex).Concat(rightFields.Select(f => f.ColumnIndex))
                    .ToList();
                break;
            }
            case JoinType.LeftSemi:
            case JoinType.LeftAnti:
            {
                var allFields = left.Fields
                    .Select((f, i) => (Field: f, ColumnIndex: new ColumnIndex(i, JoinSide.Left)))
                    .ToList();

                fields = allFields.Select(f => f.Field).ToList();
                columnIndices = allFields.Select(f => f.ColumnIndex).ToList();
                break;
            }
            case JoinType.RightSemi:
            case JoinType.RightAnti:
            {
                var allFields = left.Fields
                    .Select((f, i) => (Field: f, ColumnIndex: new ColumnIndex(i, JoinSide.Right)))
                    .ToList();

                fields = allFields.Select(f => f.Field).ToList();
                columnIndices = allFields.Select(f => f.ColumnIndex).ToList();
                break;
            }

            default:
                throw new NotImplementedException("BuildJoinSchema join type not implemented yet");
        }

        return (new Schema(fields), columnIndices);
    }

    private static QualifiedField OutputJoinField(QualifiedField oldField, JoinType joinType, bool isLeft)
    {
        var forceNullable = joinType switch
        {
            JoinType.Inner
                or JoinType.LeftSemi
                or JoinType.RightSemi
                or JoinType.LeftAnti
                or JoinType.RightAnti => false,

            JoinType.Left => !isLeft,
            JoinType.Right => isLeft,
            JoinType.Full => true
        };

        //// TODO Field
        //if (forceNullable)
        //{
        //    return oldField.WithNullable(true);
        //}

        return oldField;
    }
}