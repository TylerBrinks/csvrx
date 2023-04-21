﻿namespace CsvRx.Logical.Expressions;

public record Column(string Name) : ILogicalExpression
{
    public override string ToString()
    {
        return Name;
    }
}