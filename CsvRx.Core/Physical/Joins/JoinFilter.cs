using CsvRx.Core.Data;
using CsvRx.Core.Physical.Expressions;

namespace CsvRx.Core.Physical.Joins;

internal record JoinFilter(IPhysicalExpression FilterExpression, List<ColumnIndex> ColumnIndices, Schema Schema);