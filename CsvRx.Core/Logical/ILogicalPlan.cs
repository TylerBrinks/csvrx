using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;
using SqlParser.Ast;
using Join = CsvRx.Core.Logical.Plans.Join;

namespace CsvRx.Core.Logical;

internal interface ILogicalPlan : INode
{
    Schema Schema { get; }
    List<HashSet<Column>> UsingColumns => new();

    string ToStringIndented(Indentation? indentation = null);

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
            Join j => GetJoinExpressions(j),
            //Join {Filter: { }} jf => new List<ILogicalExpression> { jf.Filter },
            //Join {On: { }} jo => jo.On
            //    .Select(j => (ILogicalExpression) new  Binary(j.Left, BinaryOperator.Eq, j.Right))
            //    .ToList(),

            _ => new List<ILogicalExpression>()
        };

        List<ILogicalExpression> GetJoinExpressions(Join join)
        {
            var expressions = join.On
                .Select(j => (ILogicalExpression) new Binary(j.Left, BinaryOperator.Eq, j.Right))
                .ToList();

            if (join.Filter != null)
            {
                expressions.Add(join.Filter);
            }

            return expressions;
        }
    }

    VisitRecursion INode.ApplyChildren(Func<INode, VisitRecursion> action)
    {
        throw new NotImplementedException();
    }

    T INode.Transform<T>(T instance, Func<T, T> func)
    {
        var transform = func as Func<ILogicalPlan, ILogicalPlan>;
        var afterOpChildren = MapChildren(this, node => node.Transform(node, transform!));

        return func((T)afterOpChildren);
    }

    ILogicalPlan WithNewInputs(List<ILogicalPlan> inputs)
    {
        return FromPlan(GetExpressions(), inputs);
    }

    ILogicalPlan FromPlan(List<ILogicalExpression> expressions, List<ILogicalPlan> inputs)
    {
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
            var expressionCount = join.On.Count;
            var newOn = expressions.Take(expressionCount)
                .Select(expr =>
                {
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

            return new Join(inputs[0], inputs[1], newOn, filterExpression, join.JoinType, join.JoinConstraint);//, schema);
        }
    }
}

internal interface ILogicalPlanParent : ILogicalPlan
{
    ILogicalPlan Plan { get; }
}