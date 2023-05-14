namespace CsvRx.Core.Logical;

public record TableReference(string Name, string? Alias = null)
{
    public override string ToString()
    {
        return Name + (Alias != null ? $" AS {Alias}" : "");
    }
}