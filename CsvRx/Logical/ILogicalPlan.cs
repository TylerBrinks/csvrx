﻿using CsvRx.Data;
using CsvRx.Logical.Plans;

namespace CsvRx.Logical;

public interface ILogicalPlan : INode
{
    Schema Schema { get; }

    string ToStringIndented(Indentation? indentation);

    T INode.MapChildren<T>(T instance, Func<T, T> map)
    {
        var oldChildren = GetInputs();

        var newChildren = oldChildren
            .Select(p => p.Transform(p, map as Func<ILogicalPlan, ILogicalPlan>))
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

            _ => new List<ILogicalPlan>()
        };
    }

    List<ILogicalExpression> GetExpressions()
    {
        return this switch
        {
            Aggregate a => a.AggregateExpressions.Select(_=>_).Concat(a.GroupExpressions).ToList(),
            Filter f => new List<ILogicalExpression> { f.Predicate },
            Projection p => p.Expr,

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

    public ILogicalPlan WithNewInputs(List<ILogicalPlan> newInputs)
    {
        return FromPlan(GetExpressions(), newInputs);
    }

    public ILogicalPlan FromPlan(List<ILogicalExpression> expressions, List<ILogicalPlan> inputs)
    {
        switch (this)
        {
            case Projection p:
                return null;
            case Filter f:
                return null;
            case Aggregate a:
                return null;

            case TableScan t:
                return t;  // Not using filters; no need to clone.

            default:
                throw new NotImplementedException();
        }
    }
}