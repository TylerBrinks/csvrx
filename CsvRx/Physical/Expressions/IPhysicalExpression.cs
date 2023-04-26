using CsvRx.Data;
using CsvRx.Physical.Execution;

namespace CsvRx.Physical.Expressions;

public interface IPhysicalExpression
{
    ColumnDataType GetDataType(Schema schema);

    ColumnValue Evaluate(RecordBatch batch);
}

public abstract record AggregateExpression(IPhysicalExpression InputExpression) : IPhysicalExpression
{
    public ColumnDataType GetDataType(Schema schema)
    {
        throw new NotImplementedException();
    }

    public abstract ColumnValue Evaluate(RecordBatch batch);

    public virtual List<Field> StateFields { get; } = new();

    public virtual Field Field { get; }

    public abstract Accumulator CreateAccumulator();
}

public record CountFunction(IPhysicalExpression InputExpression, string Name, ColumnDataType DataType) : AggregateExpression(InputExpression)
{
    public override List<Field> StateFields => new() { new($"{Name}[count]", DataType) };

    public override Field Field => new (Name, DataType);

    public override ColumnValue Evaluate(RecordBatch batch)
    {
        throw new NotImplementedException();
    }

    public override Accumulator CreateAccumulator()
    {
        return new CountAccumulator();
    }
}

public record SumFunction(IPhysicalExpression InputExpression, string Name, ColumnDataType DataType) : AggregateExpression(InputExpression)
{
    public override List<Field> StateFields => new() { new($"{Name}[sum]", DataType) };
    public override Field Field => new (Name, DataType);

    public override ColumnValue Evaluate(RecordBatch batch)
    {
        throw new NotImplementedException();
    }

    public override Accumulator CreateAccumulator()
    {
        return new SumAccumulator();
    }
}

public record MinFunction(IPhysicalExpression InputExpression, string Name, ColumnDataType DataType) : AggregateExpression(InputExpression)
{
    public override List<Field> StateFields => new() { new($"{Name}[min]", DataType) };
    public override Field Field => new (Name, DataType);

    public override ColumnValue Evaluate(RecordBatch batch)
    {
        throw new NotImplementedException();
    }

    public override Accumulator CreateAccumulator()
    {
        return new MinAccumulator();
    }
}

public record MaxFunction(IPhysicalExpression InputExpression, string Name, ColumnDataType DataType) : AggregateExpression(InputExpression)
{
    public override List<Field> StateFields => new () { new ($"{Name}[max]", DataType)};
    public override Field Field => new (Name, DataType);

    public override ColumnValue Evaluate(RecordBatch batch)
    {
        throw new NotImplementedException();
    }

    public override Accumulator CreateAccumulator()
    {
        return new MaxAccumulator();
    }
}

public abstract record Accumulator
{
    public abstract void Accumulate(object value);

    public abstract object Value { get; }
}

public record MaxAccumulator : Accumulator
{
    private object _value = null!;

    public override void Accumulate(object value)
    {
        if (value != null)
        {
            if (_value == null)
            {
                _value = value;
            }
            else
            {
                switch (value)
                {
                    case int i when i > (int)_value:
                        _value = i;
                        break;

                    case decimal d when d > (decimal)_value:
                        _value = d;
                        break;
                }
            }
        }
    }

    public override object Value => _value;
}

public record MinAccumulator : Accumulator
{
    private object _value = null!;

    public override void Accumulate(object value)
    {
        if (value != null)
        {
            if (_value == null)
            {
                _value = value;
            }
            else
            {
                switch (value)
                {
                    case int i when i < (int)_value:
                        _value = i;
                        break;

                    case decimal d when d < (decimal)_value:
                        _value = d;
                        break; 
                    
                    default:
                        throw new NotImplementedException();
                }
            }
        }
    }

    public override object Value => _value;
}

public record SumAccumulator : Accumulator
{
    public override void Accumulate(object value)
    {
        throw new NotImplementedException();
    }
    public override object Value => null;
}

public record CountAccumulator : Accumulator
{
    public override void Accumulate(object value)
    {
        throw new NotImplementedException();
    }

    public override object Value => null;
}