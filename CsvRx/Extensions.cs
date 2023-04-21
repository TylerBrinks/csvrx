using System.Runtime.CompilerServices;
using CsvRx.Logical.Expressions;
using CsvRx.Logical.Functions;
using CsvRx.Logical;

namespace CsvRx
{
    internal static class Extensions
    {
        internal static string CreateName(this ILogicalExpression expr)
        {
            return expr switch
            {
                //Alias
                Column c => c.Name, //{}.{}
                BinaryExpr b => $"{CreateName(b.Left)} {b.Op} {CreateName(b.Right)}",
                AggregateFunction fn => GetFunctionName(fn, false, fn.Args),
                LiteralExpression l => l.Value,
                //Like
                // Case
                // cast
                // not
                // is null
                // isnotnull
                Wildcard => "*",
                _ => throw new NotImplementedException("need to implement")
            };

            //string GetBinaryName(BinaryExpr binary)
            //{
            //    var left = CreateName(binary.Left);
            //    var right = CreateName(binary.Right);
            //    return $"{left} {binary.Op} {right}";
            //}

            string GetFunctionName(AggregateFunction fn, bool distinct, List<ILogicalExpression> args)
            {
                var names = args.Select(CreateName).ToList();
                var distinctName = distinct ? "DISTINCT " : string.Empty;
                var functionName = fn.FunctionType.ToString().ToUpperInvariant();
                return $"{functionName}({distinctName}{string.Join(",", names)})";
            }
        }

        // ReSharper disable once IdentifierTypo
        internal static ILogicalExpression Rewrite(this ILogicalExpression expr, IRewriter<ILogicalExpression> rewriter)
        {
            var afterOpChildren = expr.MapChildren(expr, e => e.Rewrite(rewriter));

            // if need to mutate

            rewriter.Rewrite(afterOpChildren);

            return afterOpChildren;
        }
    }
}

//internal interface IRewriter<T>
//{
//    T Rewrite<T>(T value);
//}