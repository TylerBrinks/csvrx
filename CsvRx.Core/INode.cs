namespace CsvRx.Core;

public enum VisitRecursion
{
    Continue,
    Skip,
    Stop
}

public interface INode
{
    public VisitRecursion Apply(Func<INode, VisitRecursion> action)
    {
        var result = action(this);

        return result switch
        {
            VisitRecursion.Skip => VisitRecursion.Continue,
            VisitRecursion.Stop => VisitRecursion.Stop,
            _ => ApplyChildren(node => node.Apply(action))
        };
    }

    public T Transform<T>(T instance, Func<T, T> func) where T : INode;

    public VisitRecursion ApplyChildren(Func<INode, VisitRecursion> action);

    public T MapChildren<T>(T instance, Func<T, T> transformation) where T : INode;
}