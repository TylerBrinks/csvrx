using CsvRx.Data;

namespace CsvRx.Logical;

internal record EmptyRelation(bool ProduceOneRow = false) : ILogicalPlan
{
    public Schema Schema => new (new List<Field>());

    public override string ToString()
    {
        return "Empty Relation";
    }

    public string ToStringIndented(Indentation? indentation)
    {
        return ToString();
    }
}