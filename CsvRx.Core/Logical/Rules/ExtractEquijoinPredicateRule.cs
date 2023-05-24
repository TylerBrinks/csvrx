using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;
using SqlParser.Ast;
using Join = CsvRx.Core.Logical.Plans.Join;

namespace CsvRx.Core.Logical.Rules;

internal class ExtractEquijoinPredicateRule : ILogicalPlanOptimizationRule
{
    public ApplyOrder ApplyOrder => ApplyOrder.BottomUp;

    public ILogicalPlan? TryOptimize(ILogicalPlan plan)
    {
        if (plan is not Join join)
        {
            return null;
        }

        if (join.Filter == null)
        {
            return null;
        }

        var leftSchema = join.Plan.Schema;
        var rightSchema = join.Right.Schema;

        var (equijoinPredicates, nonEquijoinExpression) = SplitJoinPredicate(join.Filter!, leftSchema, rightSchema);

        if (!equijoinPredicates.Any())
        {
            return join;
        }

        var newOn = join.On.ToList();
        newOn.AddRange(equijoinPredicates);

        return join with {On = newOn, Filter = nonEquijoinExpression};
    }

    private static (List<(ILogicalExpression, ILogicalExpression)> Predicates, ILogicalExpression? Expression) SplitJoinPredicate(
        ILogicalExpression filter, Schema leftSchema, Schema rightSchema)
    {
        var expressions = SplitConjunction(filter);

        var joinKeys = new List<(ILogicalExpression, ILogicalExpression)>();
        var filters = new List<ILogicalExpression>();

        foreach (var expr in expressions)
        {
            if (expr is Binary { Op: BinaryOperator.Eq } b)
            {
                var left = b.Left;
                var right = b.Right;

                var (leftExpr, rightExpr) = FindValidEquijoinKeyPair(left, right, leftSchema, rightSchema);

                if (leftExpr != null && rightExpr != null)
                {
                    joinKeys.Add((leftExpr, rightExpr));
                }
                else
                {
                    filters.Add(expr);
                }
            }
            else
            {
                filters.Add(expr);
            }
        }

        ILogicalExpression? resultFilter = null;
        if (filters.Any())
        {
            resultFilter = filters.Aggregate((a, b) =>
            {
                return new Binary(a, BinaryOperator.And, b);
            });
        }

        return (joinKeys, resultFilter);
    }

    private static List<ILogicalExpression> SplitConjunction(ILogicalExpression expression)
    {
        return SplitConjunctionInternal(expression, new List<ILogicalExpression>());
    }

    private static List<ILogicalExpression> SplitConjunctionInternal(ILogicalExpression expression, List<ILogicalExpression> expressions)
    {
        if (expression is Binary { Op: BinaryOperator.And } b )
        {
            var conjunction = SplitConjunctionInternal(b.Left, expressions);
            return SplitConjunctionInternal(b.Right, conjunction);
        }

        if (expression is Alias a)
        {
            return SplitConjunctionInternal(a.Expression, expressions);
        }

        expressions.Add(expression);
        return expressions;
    }

    private static (ILogicalExpression?, ILogicalExpression?) FindValidEquijoinKeyPair(
        ILogicalExpression leftKey,
        ILogicalExpression rightKey, 
        Schema leftSchema, 
        Schema rightSchema)
    {
        var leftUsingColumns = leftKey.ToColumns();
        var rightUsingColumns = rightKey.ToColumns();

        if (!leftUsingColumns.Any() || !rightUsingColumns.Any())
        {
            return (null, null);
        }

        var leftIsLeft = CheckAllColumnsFromSchema(leftUsingColumns, leftSchema);
        var rightIsRight = CheckAllColumnsFromSchema(rightUsingColumns, rightSchema);

        return (leftIsLeft, rightIsRight) switch
        {
            (true, true) =>(leftKey, rightKey),
            (_, _) when IsSwapped() => (rightKey, leftKey),
            
            _ => (null, null)
        };

        bool IsSwapped()
        {
            return CheckAllColumnsFromSchema(rightUsingColumns, leftSchema) && 
                   CheckAllColumnsFromSchema(leftUsingColumns, rightSchema);
        }
    }

    private static bool CheckAllColumnsFromSchema(IEnumerable<Column> columns, Schema schema)
    {
        return columns
            .Select(column => schema.IndexOfColumn(column) != null)
            .All(exists => exists);
    }
}