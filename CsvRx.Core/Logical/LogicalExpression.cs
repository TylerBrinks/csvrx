using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Functions;

namespace CsvRx.Core.Logical;

internal abstract record LogicalExpression : INode
{
    VisitRecursion INode.ApplyChildren(Func<INode, VisitRecursion> action)
    {
        var children = GetChildExpressions(this);

        foreach (var child in children)
        {
            var result = action(child);

            switch (result)
            {
                case VisitRecursion.Skip:
                    return VisitRecursion.Continue;

                case VisitRecursion.Stop:
                    return result;
            }
        }

        return VisitRecursion.Continue;
    }

    internal List<LogicalExpression> GetChildExpressions(LogicalExpression expression)
    {
        return expression switch
        {
            Column or ScalarVariable => new List<LogicalExpression>(),
            BinaryExpr b => new List<LogicalExpression> { b.Left, b.Right },
            AggregateFunction fn => GetAggregateChildren(fn),
            //// Like
            //// between
            //// Case
            //// InList
            _ => new List<LogicalExpression>()
        };

        List<LogicalExpression> GetAggregateChildren(AggregateFunction fn)
        {
            var args = fn.Args.Select(_ => _).ToList();
            if (fn.Filter != null)
            {
                args.Add(fn.Filter);
            }

            return args;
        }
    }

    T INode.MapChildren<T>(T instance, Func<T, T> transformation)
    {
        throw new NotImplementedException();
    }

    T INode.Transform<T>(T instance, Func<T, T>? func)
    {
        throw new NotImplementedException();
    }
}