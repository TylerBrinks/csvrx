﻿using CsvRx.Core.Data;

namespace CsvRx.Core.Logical.Values;

internal record BooleanScalarValue(bool Value) : ScalarValue(Value, ColumnDataType.Boolean)
{
    public override string ToString()
    {
        return Value.ToString();
    }
}