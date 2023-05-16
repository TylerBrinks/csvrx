using CsvRx.Core.Physical.Expressions;

namespace CsvRx.Core.Physical.Joins;

internal record JoinOn(Column Left, Column Right);