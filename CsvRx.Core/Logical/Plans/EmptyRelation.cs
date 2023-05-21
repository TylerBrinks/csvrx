using CsvRx.Core.Data;

namespace CsvRx.Core.Logical.Plans;

internal record EmptyRelation(bool ProduceOneRow = false) : ILogicalPlan
{
    public Schema Schema => new(new List<QualifiedField>());

    public override string ToString()
    {
        return "Empty Relation";
    }

    public string ToStringIndented(Indentation? indentation = null)
    {
        return ToString();
    }
}