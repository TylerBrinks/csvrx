﻿using CsvRx.Core.Data;

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

        Assert.Equal(2l, batch.Results[0].Values[0]);
        Assert.Equal(3l, batch.Results[0].Values[1]);
        Assert.Equal(4l, batch.Results[0].Values[2]);
        Assert.Equal(2l, batch.Results[1].Values[0]);
        Assert.Equal(3l, batch.Results[1].Values[1]);
        Assert.Equal(4l, batch.Results[1].Values[2]);
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


}