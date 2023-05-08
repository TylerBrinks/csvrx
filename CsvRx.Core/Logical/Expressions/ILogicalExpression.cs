namespace CsvRx.Core.Logical.Expressions;

internal interface ILogicalExpression : INode
{
    VisitRecursion INode.ApplyChildren(Func<INode, VisitRecursion> action)
    {
        var children = GetChildExpressions(this);

        foreach (var result in children.Select(child => action(child)))
        {
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

    internal List<ILogicalExpression> GetChildExpressions(ILogicalExpression expression)
    {
        return expression switch
        {
            Column or ScalarVariable => new List<ILogicalExpression>(),
            Binary b => new List<ILogicalExpression> { b.Left, b.Right },
            AggregateFunction fn => GetAggregateChildren(fn),
            Alias a => new List<ILogicalExpression> { a.Expression },
            //// Like
            //// between
            //// Case
            //// InList
            _ => new List<ILogicalExpression>()
        };

        List<ILogicalExpression> GetAggregateChildren(AggregateFunction fn)
        {
            var args = fn.Args.ToList();
            if (fn.Filter != null)
            {
                args.Add(fn.Filter);
            }

            return args;
        }
    }

    T INode.MapChildren<T>(T instance, Func<T, T> transformation)
    {
        return this switch
        {
            _ => (T)this,
        };
    }

    T INode.Transform<T>(T instance, Func<T, T>? func)
    {
        var transformFunc = func as Func<ILogicalExpression, ILogicalExpression>;
        var afterOpChildren = MapChildren(this, node => node.Transform(node, transformFunc!));

        var newNode = func!((T)afterOpChildren);

        return newNode;
    }
}