using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;
using SqlParser.Ast;

namespace CsvRx.Core.Logical.Plans;

internal record Join(
    ILogicalPlan Plan,
    ILogicalPlan Right,
    List<(ILogicalExpression Left, ILogicalExpression Right)> On,
    ILogicalExpression? Filter,
    JoinType JoinType,
    JoinConstraint JoinConstraint
    //Schema Schema
    ) : ILogicalPlanParent
{
    internal static Join TryNew(
        ILogicalPlan left,
        ILogicalPlan right,
        JoinType joinType,
        (List<Column> Left, List<Column> Rigt) joinKeys,
        JoinConstraint joinConstraint,
        ILogicalExpression? expressionFilter)
    {
        ILogicalExpression? filter = null;
        if (expressionFilter != null)
        {
            var schemas = new List<List<Schema>> { new() { left.Schema, right.Schema } };
            filter = expressionFilter.NormalizeColumnWithSchemas(schemas, new List<HashSet<Column>>());
        }

        // join keys
        var (leftKeys, rightKeys) = GetJoinKeys(joinKeys);

        var on = leftKeys.Zip(rightKeys)
            .Select(k => ((ILogicalExpression)k.First, (ILogicalExpression)k.Second))
            .ToList();

        var joinSchema = LogicalExtensions.BuildJoinSchema(left.Schema, right.Schema, joinType);

        return new Join(left, right, on, filter, joinType, joinConstraint);//, joinSchema);

        (List<Column> Left, List<Column> Right) GetJoinKeys((List<Column> Left, List<Column> Right) joinKeyValues)
        {
            var keys = joinKeyValues.Left.Zip(joinKeyValues.Right)
                .Select(k =>
                {
                    var leftColumn = k.First;
                    var rightColumn = k.Second;

                    return (leftColumn.Relation, rightColumn.Relation) switch
                    {
                        ({ } lr, { } rr) => GetDualKeys(lr, rr),
                        //TODO implement join key patterns
                        //({}, null) => "",
                        //(null, {}) => "",
                        //(null, null) => "",
                        _ => throw new NotImplementedException()
                    };

                    (Column Left, Column Right) GetDualKeys(TableReference leftReference, TableReference rightReference)
                    {
                        var leftPlan = ((ILogicalPlanParent)left).Plan;
                        var rightPlan = ((ILogicalPlanParent)right).Plan;

                        var alIsLeft = leftPlan.Schema.FieldsWithQualifiedName(leftReference, leftColumn.Name);
                        var aColIsRight = rightPlan.Schema.FieldsWithQualifiedName(leftReference, leftColumn.Name);
                        var bIsLeft = leftPlan.Schema.FieldsWithQualifiedName(rightReference, rightColumn.Name);
                        var bIsRight = rightPlan.Schema.FieldsWithQualifiedName(rightReference, rightColumn.Name);

                        return (lIsLeft: alIsLeft, lIsRight: aColIsRight, rIsLeft: bIsLeft, rIsRight: bIsRight) switch
                        {
                            (_, { }, { }, _) => (r: rightColumn, l: leftColumn),
                            ({ }, _, _, { }) => (l: leftColumn, r: rightColumn),
                            _ => (Normalize(left, leftColumn), Normalize(right, rightColumn))
                        };
                    }

                }).ToList();

            return joinKeyValues;
        }

        Column Normalize(ILogicalPlan leftPlan, Column leftColumn)
        {
            var schema = new List<Schema> { leftPlan.Schema };
            var fallbackSchemas = left.FallbackNormalizeSchemas();
            var usingColumns = left.UsingColumns;

            var schemaList = new List<List<Schema>> { schema, fallbackSchemas };

            return leftColumn.NormalizeColumnWithSchemas(schemaList, usingColumns);
        }
    }

    public Schema Schema => Plan.Schema.Join(Right.Schema);

    public override string ToString()
    {
        var on = On.Select(_ => $"{ _.Left } = { _.Right }").ToList();
        var filter = Filter!=null? $" Filter: { Filter }" : "";
        return $"{JoinType} Join: {string.Join(",", on)}{filter}";
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        var indent = indentation ?? new Indentation();
        return $"{this} {indent.Next(Plan)}{indent.Repeat(Right)}";
    }
}