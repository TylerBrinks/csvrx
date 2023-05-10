﻿using CsvRx.Core.Logical;
using CsvRx.Core.Logical.Expressions;

namespace CsvRx.Core.Data;

public abstract record Field(string Name, ColumnDataType DataType);

public record QualifiedField(string Name, ColumnDataType DataType, TableReference? Qualifier = null) : Field(Name, DataType)
{
    internal Column QualifiedColumn()
    {
        return new Column(Name, Qualifier);
    }

    public static QualifiedField Unqualified(string name, ColumnDataType dataType)
    {
        return new QualifiedField(name, dataType);
    }
}