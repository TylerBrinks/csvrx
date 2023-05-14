using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;

namespace CsvRx.Core.Logical;

internal interface ILogicalPlan : INode
{
    Schema Schema { get; }
    List<HashSet<Column>> UsingColumns => new();

    string ToStringIndented(Indentation? indentation);

    T INode.MapChildren<T>(T instance, Func<T, T> map)
    {
        var oldChildren = GetInputs();

        var callback = map as Func<ILogicalPlan, ILogicalPlan>;
        var newChildren = oldChildren
            .Select(p => p.Transform(p, callback!))
            .ToList();

        if (oldChildren.Zip(newChildren).Any(index => index.First != index.Second))
        {
            return (T)this;
        }

        return (T)this;
    }

    List<ILogicalPlan> GetInputs()
    {
        return this switch
        {
            Join j => new List<ILogicalPlan> { j.Plan, j.Right },

            ILogicalPlanParent p => new List<ILogicalPlan> { p.Plan },

            _ => new List<ILogicalPlan>()
        };
    }

    List<ILogicalExpression> GetExpressions()
    {
        return this switch
        {
            Aggregate a => a.AggregateExpressions.ToList().Concat(a.GroupExpressions).ToList(),
            Filter f => new List<ILogicalExpression> { f.Predicate },
            Projection p => p.Expression,
            Sort s => s.OrderByExpressions,
            Join {Filter: { }} j => new List<ILogicalExpression> { j.Filter },

            _ => new List<ILogicalExpression>()
        };
    }

    VisitRecursion INode.ApplyChildren(Func<INode, VisitRecursion> action)
    {
        throw new NotImplementedException();
    }

    T INode.Transform<T>(T instance, Func<T, T> func)
    {
        var afterOpChildren = MapChildren(this, node => node.Transform(node, func as Func<ILogicalPlan, ILogicalPlan>));

        var newNode = func((T)afterOpChildren);

        return newNode;
    }

    ILogicalPlan WithNewInputs(List<ILogicalPlan> inputs)
    {
        var expressions = GetExpressions();

        switch (this)
        {
            case Projection p:
                return new Projection(inputs[0], expressions, p.Schema);

            case Filter:
                var predicate = expressions[0];
                return new Filter(inputs[0], predicate);

            case Aggregate a:
                return a with { Plan = inputs[0] };

            case TableScan t:
                return t;  // Not using filters; no need to clone.

            case Sort:
                return new Sort(inputs[0], expressions);

            case Distinct:
                return new Distinct(inputs[0]);

            case Limit l:
                return l with { Plan = inputs[0] };

            case SubqueryAlias s:
                return SubqueryAlias.TryNew(inputs[0], s.Alias);

            case Join j:
                return BuildJoin(j);

            default:
                throw new NotImplementedException("WithNewInputs not implemented for plan type");
        }


        ILogicalPlan BuildJoin(Join join)
        {
            var schema = LogicalExtensions.BuildJoinSchema(inputs[0].Schema, inputs[1].Schema, join.JoinType);

            var expressionCount = join.On.Count;
            var newOn = expressions.Take(expressionCount)
                .Select(expr =>
                {
                    // todo: unalias, implement simplify rule
                    var unaliased = expr.Unalias();

                    if (unaliased is Binary b)
                    {
                        return (b.Left, b.Right);
                    }

                    throw new InvalidOperationException("Expressions must be a binary expression.");
                })
                .ToList();

            ILogicalExpression? filterExpression = null;

            if (expressions.Count > expressionCount)
            {
                filterExpression = expressions[^1];
            }

            return new Join(inputs[0], inputs[1], newOn, filterExpression, join.JoinType, join.JoinConstraint, schema);
        }
    }


}

internal interface ILogicalPlanParent : ILogicalPlan
{
    ILogicalPlan Plan { get; }
}