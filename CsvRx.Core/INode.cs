namespace CsvRx.Core;

internal interface INode
{
    VisitRecursion Apply(Func<INode, VisitRecursion> action)
    {
        var result = action(this);

        return result switch
        {
            VisitRecursion.Skip => VisitRecursion.Continue,
            VisitRecursion.Stop => VisitRecursion.Stop,
            _ => ApplyChildren(node => node.Apply(action))
        };
    }

    internal T Transform<T>(T instance, Func<T, T> func) where T : INode;

    internal VisitRecursion ApplyChildren(Func<INode, VisitRecursion> action);

    internal T MapChildren<T>(T instance, Func<T, T> transformation) where T : INode;
}