using System.Collections;
using CsvRx.Core.Data;

namespace CsvRx.Tests.Data;

public class RecordBatchTests
{
    [Fact]
    public void RecordBatch_Gets_Array_Types()
    {
        Assert.IsType<StringArray>(RecordBatch.GetArrayType(new QualifiedField("", ColumnDataType.Utf8)));
        Assert.IsType<IntegerArray>(RecordBatch.GetArrayType(new QualifiedField("", ColumnDataType.Integer)));
        Assert.IsType<DoubleArray>(RecordBatch.GetArrayType(new QualifiedField("", ColumnDataType.Double)));
        Assert.IsType<BooleanArray>(RecordBatch.GetArrayType(new QualifiedField("", ColumnDataType.Boolean)));
        Assert.IsType<StringArray>(RecordBatch.GetArrayType(new QualifiedField("", ColumnDataType.TimestampMicrosecond)));
        Assert.IsType<StringArray>(RecordBatch.GetArrayType(null));
    }

    [Fact]
    public void RecordBatch_Sets_Array_Types()
    {
        var batch = new RecordBatch(new Schema(new List<QualifiedField>
        {
            new ("", ColumnDataType.Utf8),
            new ("", ColumnDataType.Integer),
            new ("", ColumnDataType.Double),
            new ("", ColumnDataType.Boolean),
            new ("", ColumnDataType.Date32),
        }));

        Assert.IsType<StringArray>(batch.Results[0]);
        Assert.IsType<IntegerArray>(batch.Results[1]);
        Assert.IsType<DoubleArray>(batch.Results[2]);
        Assert.IsType<BooleanArray>(batch.Results[3]);
        Assert.IsType<StringArray>(batch.Results[4]);
    }

    [Fact]
    public void RecordBatch_Counts_Rows()
    {
        var batch = new RecordBatch(new Schema(new List<QualifiedField>
        {
            new ("", ColumnDataType.Utf8),
        }));

        batch.Results[0].Values.Add("");
        batch.Results[0].Values.Add("");
        batch.Results[0].Values.Add("");

        Assert.Equal(3, batch.RowCount);
    }

    [Fact]
    public void RecordBatch_Reorders_Columns()
    {
        var batch = new RecordBatch(new Schema(new List<QualifiedField>
        {
            new ("ordered", ColumnDataType.Utf8),
            new ("unordered", ColumnDataType.Utf8),
        }));

        batch.Results[0].Values.Add("c");
        batch.Results[0].Values.Add("b");
        batch.Results[0].Values.Add("a");

        batch.Results[1].Values.Add("c");
        batch.Results[1].Values.Add("b");
        batch.Results[1].Values.Add("a");

        batch.Reorder(new List<int> { 2, 1, 0 }, new List<int> { 1 });

        Assert.Equal("a", batch.Results[0].Values[0]);
        Assert.Equal("b", batch.Results[0].Values[1]);
        Assert.Equal("c", batch.Results[0].Values[2]);
        Assert.Equal("c", batch.Results[1].Values[0]);
        Assert.Equal("b", batch.Results[1].Values[1]);
        Assert.Equal("a", batch.Results[1].Values[2]);
    }

    [Fact]
    public void RecordBatch_Slices_Values()
    {
        var batch = new RecordBatch(new Schema(new List<QualifiedField>
        {
            new ("", ColumnDataType.Integer),
            new ("", ColumnDataType.Integer),
        }));

        foreach (var array in batch.Results)
        {
            for (var i = 1; i < 6; i++)
            {
                array.Add(i);
            }
        }

        batch.Slice(1, 3);

        Assert.Equal(2L, batch.Results[0].Values[0]);
        Assert.Equal(3L, batch.Results[0].Values[1]);
        Assert.Equal(4L, batch.Results[0].Values[2]);
        Assert.Equal(2L, batch.Results[1].Values[0]);
        Assert.Equal(3L, batch.Results[1].Values[1]);
        Assert.Equal(4L, batch.Results[1].Values[2]);
    }

    [Fact]
    public void RecordBatch_Repartitions()
    {
        var batch = new RecordBatch(new Schema(new List<QualifiedField>
        {
            new ("", ColumnDataType.Integer),
            new ("", ColumnDataType.Integer)
        }));

        foreach (var array in batch.Results)
        {
            for (var i = 0; i < 10; i++)
            {
                array.Add(i);
            }
        }

        var partitions = batch.Repartition(3).ToList();

        Assert.Equal(3, partitions[0].RowCount);
        Assert.Equal(3, partitions[1].RowCount);
        Assert.Equal(3, partitions[2].RowCount);
        Assert.Equal(1, partitions[3].RowCount);
    }

    [Fact]
    public void RecordBatch_Creates_Instances_With_Data()
    {
        var schema = new Schema(new List<QualifiedField>
        {
            new("first", ColumnDataType.Integer),
            new("second", ColumnDataType.Integer)
        });

        var columns = new List<object> { 1, 2 };

        var batch = RecordBatch.TryNew(schema, columns!);

        Assert.Equal(2, batch.Results.Count);
        Assert.Equal(1, batch.RowCount);
        Assert.Equal(1L, batch.Results[0].Values[0]);
        Assert.Equal(2L, batch.Results[1].Values[0]);
    }

    [Fact]
    public void RecordBatch_Creates_Instances_With_List_Data()
    {
        var schema = new Schema(new List<QualifiedField>
        {
            new("first", ColumnDataType.Integer),
            new("second", ColumnDataType.Integer)
        });

        var lists = new List<IList>
        {
            new List<int> {1, 2, 3},
            new List<int> {4, 5, 6},
        };

        var batch = RecordBatch.TryNewWithLists(schema, lists);

        Assert.Equal(2, batch.Results.Count);
        Assert.Equal(3, batch.RowCount);
        Assert.Equal(1L, batch.Results[0].Values[0]);
        Assert.Equal(2L, batch.Results[0].Values[1]);
        Assert.Equal(3L, batch.Results[0].Values[2]);
        Assert.Equal(4L, batch.Results[1].Values[0]);
        Assert.Equal(5L, batch.Results[1].Values[1]);
        Assert.Equal(6L, batch.Results[1].Values[2]);
    }

    [Fact]
    public void RecordBatch_Concatenates_Batches()
    {
        var schema = new Schema(new List<QualifiedField>
        {
            new("first", ColumnDataType.Integer),
            new("second", ColumnDataType.Integer)
        });

        var columns1 = new List<object> { 1, 2 };
        var columns2 = new List<object> { 3, 4 };

        var batch1 = RecordBatch.TryNew(schema, columns1!);
        var batch2 = RecordBatch.TryNew(schema, columns2!);
        batch1.Concat(batch2);

        Assert.Equal(2, batch1.Results.Count);
        Assert.Equal(2, batch1.RowCount);
        Assert.Equal(1L, batch1.Results[0].Values[0]);
        Assert.Equal(3L, batch1.Results[0].Values[1]);
        Assert.Equal(2L, batch1.Results[1].Values[0]);
        Assert.Equal(4L, batch1.Results[1].Values[1]);
    }

    [Fact]
    public void RecordBatch_Counts_Zero_Rows()
    {
        var schema = new Schema(new List<QualifiedField>(new List<QualifiedField> {new("", ColumnDataType.Integer)}));
        Assert.Equal(0, new RecordBatch(schema).RowCount);
    }

    [Fact]
    public void RecordBath_Prevents_Schema_Mismatch()
    {
        var schema = new Schema(new List<QualifiedField> {new("name", ColumnDataType.Integer)});
        Assert.Throws<InvalidOperationException>(() => RecordBatch.TryNew(schema, new List<object?>()));
    }

    [Fact]
    public void RecordBath_Counts_Rows()
    {
        var schema = new Schema(new List<QualifiedField> { new("name", ColumnDataType.Integer) });
        var batch = new RecordBatch(schema);

        Assert.Equal(0, batch.RowCount);

        batch.Results[0].Add("");
       
        Assert.Equal(1, batch.RowCount);
    }
}