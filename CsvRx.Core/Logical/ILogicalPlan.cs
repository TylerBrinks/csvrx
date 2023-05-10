﻿using CsvRx.Core.Data;
using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;

namespace CsvRx.Core.Logical;

internal interface ILogicalPlan : INode
{
    Schema Schema { get; }
    List<HashSet<Column>> UsingColumns => new ();

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
            return (T) this;
        }

        return (T)this;
    }

    List<ILogicalPlan> GetInputs()
    {
        return this switch
        {
            Aggregate a => new List<ILogicalPlan>{ a.Plan },
            Distinct d => new List<ILogicalPlan> { d.Plan },
            Filter f => new List<ILogicalPlan> { f.Plan },
            Projection p => new List<ILogicalPlan> { p.Plan },
            Sort s => new List<ILogicalPlan>{ s.Plan },
            Limit l => new List<ILogicalPlan>{ l.Plan },

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
                return a with {Plan = inputs[0]};

            case TableScan t:
                return t;  // Not using filters; no need to clone.

            case Sort:
                return new Sort(inputs[0], expressions);

            case Distinct:
                return new Distinct(inputs[0]);

            case Limit l:
                return l with {Plan = inputs[0]};

            default:
                throw new NotImplementedException("WithNewInputs not implemented for plan type");
        }
    }
}

internal interface ILogicalPlanParent : ILogicalPlan
{
    ILogicalPlan Plan { get; }
}