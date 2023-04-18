using System.Drawing;
using CsvRx.Data;
using CsvRx.Logical.Expressions;

namespace CsvRx.Logical;

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

        switch (result)
        {
            case VisitRecursion.Skip:
                return VisitRecursion.Continue;

            case VisitRecursion.Stop:
                return VisitRecursion.Stop;
        }

        return ApplyChildren(node => node.Apply(action));
    }

    public VisitRecursion ApplyChildren(Func<INode, VisitRecursion> action);
}

public interface ILogicalPlan// : IFriendlyFormat
{
    Schema Schema { get; }

    string ToStringIndented(Indentation? indentation);
}

public record Indentation(int Size = 0)
{
    public int Size { get; set; } = Size;

    public string Next(ILogicalPlan plan)
    {
        Size += 1;

        return Environment.NewLine + new string(' ', Size * 2) + plan.ToStringIndented(this);
    }
}

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
}