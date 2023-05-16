
namespace CsvRx.Core
{
    internal static class Extensions
    {
        internal static long?[] AsNullable(this IEnumerable<long> source)
        {
            return source.Cast<long?>().ToArray();
        }
    }
}
