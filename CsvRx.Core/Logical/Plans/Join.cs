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
    JoinConstraint JoinConstraint,
    Schema Schema
    ) : ILogicalPlanParent
{
    internal static Join TryNew(
        ILogicalPlan Left,
        ILogicalPlan Right,
        JoinType JoinType,
        (List<Column> Left, List<Column> Rigt) JoinKeys,
        JoinConstraint JoinConstraint,
        ILogicalExpression? Filter)
    {
        ILogicalExpression? filter = null;
        if (Filter != null)
        {
            var schemas = new List<List<Schema>> { new() { Left.Schema, Right.Schema } };
            filter = Filter.NormalizeColumnWithSchemas(schemas, new List<HashSet<Column>>());
        }

        // join keys
        var (leftKeys, rightKeys) = GetJoinKeys(JoinKeys);

        var on = leftKeys.Zip(rightKeys)
            .Select(k => ((ILogicalExpression)k.First, (ILogicalExpression)k.Second))
            .ToList();

        var joinSchema = LogicalExtensions.BuildJoinSchema(Left.Schema, Right.Schema, JoinType);

        return new Join(Left, Right, on, filter, JoinType, JoinConstraint, joinSchema);


        (List<Column> Left, List<Column> Right) GetJoinKeys((List<Column> Left, List<Column> Right) joinKeys)
        {
            var keys = joinKeys.Left.Zip(joinKeys.Right)
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

                    (Column Left, Column Right) GetDualKeys(TableReference left, TableReference right)
                    {
                        var leftPlan = ((ILogicalPlanParent)Left).Plan;
                        var rightPlan = ((ILogicalPlanParent)Right).Plan;

                        var alIsLeft = leftPlan.Schema.FieldsWithQualifiedName(left, leftColumn.Name);
                        var aColIsRight = rightPlan.Schema.FieldsWithQualifiedName(left, leftColumn.Name);
                        var bIsLeft = leftPlan.Schema.FieldsWithQualifiedName(right, rightColumn.Name);
                        var bIsRight = rightPlan.Schema.FieldsWithQualifiedName(right, rightColumn.Name);

                        return (lIsLeft: alIsLeft, lIsRight: aColIsRight, rIsLeft: bIsLeft, rIsRight: bIsRight) switch
                        {
                            (_, { }, { }, _) => (r: rightColumn, l: leftColumn),
                            ({ }, _, _, { }) => (l: leftColumn, r: rightColumn),
                            _ => (Normalize(Left, leftColumn), Normalize(Right, rightColumn))
                        };
                    }

                }).ToList();

            return joinKeys;
        }

        Column Normalize(ILogicalPlan left, Column leftColumn)
        {
            var schema = new List<Schema> { left.Schema };
            var fallbackSchemas = Left.FallbackNormalizeSchemas();
            var usingColumns = Left.UsingColumns;

            var schemaList = new List<List<Schema>> { schema, fallbackSchemas };
            return leftColumn.NormalizeColumnWithSchemas(schemaList, usingColumns);
        }
    }

    public Schema Schema => Plan.Schema.Join(Right.Schema);

    public override string ToString()
    {
        return $"Join: {Filter}";
    }

    public string ToStringIndented(Indentation? indentation)
    {
        var indent = indentation ?? new Indentation();
        return $"{this} {indent.Next(Plan)}{indent.Repeat(Right)}";
    }
}