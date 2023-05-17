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
        var transform = (Func<ILogicalExpression, ILogicalExpression>)transformation ;
        
        return (this switch
        {
            Alias a => a with {Expression = transform(a.Expression)} as T,
            Binary b => new Binary(transform(b.Left), b.Op, transform(b.Right)) as T,
            AggregateFunction fn => (fn with {Args = TransformList(fn.Args, transform)}) as T,
            _ => (T)this,
        })!;


        List<ILogicalExpression> TransformList(IEnumerable<ILogicalExpression> list, Func<ILogicalExpression, ILogicalExpression> func)
        {
            return list.Select(_ => _.Transform(_, func)).ToList();
        }
    }

    T INode.Transform<T>(T instance, Func<T, T>? func)
    {
        var transformFunc = func as Func<ILogicalExpression, ILogicalExpression>;
        var afterOpChildren = MapChildren(this, node => node.Transform(node, transformFunc!));

        var newNode = func!((T)afterOpChildren);

        return newNode;
    }
}