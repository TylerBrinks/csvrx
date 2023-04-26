namespace CsvRx.Core.Physical.Aggregation;

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