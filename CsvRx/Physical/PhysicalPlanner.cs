using CsvRx.Data;
using CsvRx.Logical;
using CsvRx.Logical.Expressions;
using CsvRx.Logical.Plans;

namespace CsvRx.Physical
{
    public class PhysicalPlanner
    {
        public IExecutionPlan CreateInitialPlan(ILogicalPlan optimized)
        {
            switch (optimized)
            {
                case TableScan t:
                {
                    return null;
                }

                case Aggregate a:
                {
                    var inputExec = CreateInitialPlan(a.Plan);
                    var physicalInputSchema = inputExec.Schema;
                    var logicalInputSchema = a.Plan.Schema;

                    //var groups = CreateGroupingPhysicalExpr(a.GroupExpressions, logicalInputSchema, physicalInputSchema);

                    //var aggregates = a.AggregateExpressions.Select(e => 
                    //    CreateAggregateExpression(e, logicalInputSchema, physicalInputSchema)).ToList();
                    return null;
                }
                case Projection a:
                {
                    var inputExec = CreateInitialPlan(a.Plan);
                    var inputSchema = a.Plan.Schema;

                    var physicalExprs = a.Expr.Select(e =>
                    {
                        string physicalName = null!;

                        if (e is Column col)
                        {
                            var index = inputSchema.IndexOfColumn(col);
                            if (index != null)
                            {
                                physicalName = inputExec.Schema.Fields[index.Value].Name;
                            }
                            else
                            {
                                physicalName = GetPhysicalName(e);
                            }
                        }
                        else
                        {
                            physicalName = GetPhysicalName(e);
                        }

                        return CreatePhysicalExpr(e, inputSchema, inputExec.Schema);
                    }).ToList();
                    return new PhysicalProjection(physicalExprs, inputExec);
                }


                case Filter f:
                    return null;

                case Distinct d:
                    return null;

                default:
                    throw new NotImplementedException();
            };
        }

        #region Physical Expression
        private IPhysicalExpression CreatePhysicalExpr(ILogicalExpression expression, Schema inputSchema, Schema inputExecSchema)
        {
            switch (expression)
            {
                case Column c:
                    var index = inputSchema.IndexOfColumn(c);
                    return new PhysicalColumn(c.Name, index!.Value);

                default:
                    throw new NotImplementedException();
            };
        }

        private string GetPhysicalName(ILogicalExpression expr)
        {
            return expr switch
            {
                Column c => c.Name,
                //BinaryExpr b => $"{GetPhysicalName} {} {GetPhysicalName}"
                _ => throw new NotImplementedException()
            };
        }

        #endregion
    }

    public interface IPhysicalExpression
    {
    }

    public record PhysicalColumn(string Name, int Index) : IPhysicalExpression;

    public record PhysicalProjection(List<IPhysicalExpression> PhysicalExprs, IExecutionPlan InputExec) : IExecutionPlan
    {
        public Schema Schema { get; }
    }
}
