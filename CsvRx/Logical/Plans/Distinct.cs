﻿using CsvRx.Data;

namespace CsvRx.Logical.Plans;

internal record Distinct(ILogicalPlan Plan) : ILogicalPlanWrapper
{
    public Schema Schema => Plan.Schema;

    public string ToStringIndented(Indentation? indentation)
    {
        var indent = indentation ?? new Indentation();
        return $"Distinct {indent.Next(Plan)}";
    }
}