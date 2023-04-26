﻿namespace CsvRx.Core.Logical;

public record Indentation(int Size = 0)
{
    public int Size { get; set; } = Size;

    public string Next(ILogicalPlan plan)
    {
        Size += 1;

        return Environment.NewLine + new string(' ', Size * 2) + plan.ToStringIndented(this);
    }
}