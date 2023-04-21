using CsvRx.Logical.Expressions;

namespace CsvRx.Logical;

public interface ILogicalExpression : INode
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

    List<ILogicalExpression> GetChildExpressions(ILogicalExpression expression)
    {
        return expression switch
        {
            Column or ScalarVariable => new List<ILogicalExpression>(),
            BinaryExpr b => new List<ILogicalExpression> { b.Left, b.Right },

            //// Like
            //// between
            //// Case
            //// Aggregate fn
            //// InList
            _ => new List<ILogicalExpression>()
        };
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