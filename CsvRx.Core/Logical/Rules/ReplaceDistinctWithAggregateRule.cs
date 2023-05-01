﻿using CsvRx.Core.Logical.Expressions;
using CsvRx.Core.Logical.Plans;

namespace CsvRx.Core.Logical.Rules
{
    internal class ReplaceDistinctWithAggregateRule : ILogicalPlanOptimizationRule
    {
        public ApplyOrder ApplyOrder => ApplyOrder.BottomUp;

        public ILogicalPlan? TryOptimize(ILogicalPlan plan)
        {
            if (plan is Distinct d)
            {
                var groupExpression = LogicalExtensions.ExpandWildcard(plan.Schema);
                return Aggregate.TryNew(d.Plan, groupExpression, new List<ILogicalExpression>());
            }

            return null;
        }
    }
}
