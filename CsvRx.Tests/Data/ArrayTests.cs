using CsvRx.Core.Data;

namespace CsvRx.Tests.Data;

public class ArrayTests
{
    [Fact]
    public void BooleanArray_Adds_Values()
    {
        var array = new BooleanArray();
        array.Add(true);
        array.Add("true");
        array.Add(1);
        array.Add(false);
        array.Add("false");
        array.Add(0);
        array.Add("abc");
        array.Add(123);
        array.Add(null);

        Assert.Equal(9, array.Values.Count);
        Assert.Equal(2, array.Values.Cast<bool?>().Count(_ => _ != null && _.Value));
        Assert.Equal(2, array.Values.Cast<bool?>().Count(_ => _ != null && !_.Value));
        Assert.Equal(5, array.Values.Cast<bool?>().Count(_ => _ == null));
    }

    [Fact]
    public void BooleanArray_Fills_Null_Values()
    {
        var array = new BooleanArray().FillWithNull(5);

        Assert.Equal(5, array.Values.Cast<bool?>().Count(_ => _ == null));
    }

    [Fact]
    public void IntegerArray_Adds_Values()
    {
        var array = new IntegerArray();
        array.Add(1);
        array.Add("1");
        array.Add("abc");
        array.Add(123.45);
        array.Add(null);

        Assert.Equal(5, array.Values.Count);
        Assert.Equal(2, array.Values.Cast<long?>().Count(_ => _ != null));
        Assert.Equal(3, array.Values.Cast<long?>().Count(_ => _ == null));
    }

    [Fact]
    public void IntegerArray_Fills_Null_Values()
    {
        var array = new IntegerArray().FillWithNull(5);

        Assert.Equal(5, array.Values.Cast<long?>().Count(_ => _ == null));
    }

    [Fact]
    public void DoubleArray_Adds_Values()
    {
        var array = new DoubleArray();
        array.Add(1);
        array.Add(1.23);
        array.Add("2.34");
        array.Add("abc");
        array.Add(null);

        Assert.Equal(5, array.Values.Count);
        Assert.Equal(3, array.Values.Cast<double?>().Count(_ => _ != null));
        Assert.Equal(2, array.Values.Cast<double?>().Count(_ => _ == null));
    }

    [Fact]
    public void DoubleArray_Fills_Null_Values()
    {
        var array = new DoubleArray().FillWithNull(5);

        Assert.Equal(5, array.Values.Cast<double?>().Count(_ => _ == null));
    }

    [Fact]
    public void TypedArray_Gets_Sorted_Indices()
    {
        var array = new StringArray();
        array.Add("c");
        array.Add("b");
        array.Add("d");
        array.Add("e");
        array.Add("a");
        var indices = array.GetSortIndices(true);
        Assert.True(new List<int> { 2, 1, 3, 4, 0 }.SequenceEqual(indices));
    }

    [Fact]
    public void TypedArray_Gets_Sorted_Indices_Descending()
    {
        var array = new StringArray();
        array.Add("c");
        array.Add("b");
        array.Add("d");
        array.Add("e");
        array.Add("a");
        var indices = array.GetSortIndices(false);
        Assert.True(new List<int> { 2, 3, 1, 0, 4 }.SequenceEqual(indices));
    }

    [Fact]
    public void TypedArray_Concatenates_Values()
    {
        var array = new StringArray();
        array.Add("a");
        array.Add("b");
        
       array.Concat(new List<string>{"c", "d", "e"});

       Assert.True(new List<string>{"a", "b", "c", "d", "e" }.SequenceEqual(array.Values.Cast<string>()));
    }

    [Fact]
    public void TypedArray_Reorders_Lists()
    {
        var array = new StringArray();
        array.Add("a");
        array.Add("b");
        array.Add("c");
        array.Add("d");
        array.Add("e");

        array.Reorder(new List<int>{4,3,2,1,0});

        Assert.True(new List<string> { "e", "d", "c", "b", "a"  }.SequenceEqual(array.Values.Cast<string>()));
    }

    [Fact]
    public void TypedArray_Slices_Values()
    {
        var array = new StringArray();
        array.Add("a");
        array.Add("b");
        array.Add("c");
        array.Add("d");
        array.Add("e");

        array.Slice(1, 3);

        Assert.True(new List<string> { "b", "c", "d" }.SequenceEqual(array.Values.Cast<string>()));
    }
}