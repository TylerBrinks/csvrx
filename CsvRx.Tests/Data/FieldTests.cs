using CsvRx.Core.Data;
using CsvRx.Core.Logical;

namespace CsvRx.Tests.Data;

public class FieldTests
{
    [Fact]
    public void Fields_Creates_Columns()
    {
        var field = new QualifiedField("name", ColumnDataType.Integer);
        var column = field.QualifiedColumn();

        Assert.Equal("name", column.Name);
        Assert.Null(column.Relation);
    }

    [Fact]
    public void Fields_Creates_Qualified_Columns()
    {
        var field = new QualifiedField("name", ColumnDataType.Integer, new TableReference("table"));
        var column = field.QualifiedColumn();

        Assert.Equal("name", column.Name);
        Assert.NotNull(column.Relation);
    }

    [Fact]
    public void Fields_Creates_Unqualified_Columns()
    {
        var column = QualifiedField.Unqualified("name", ColumnDataType.Integer);

        Assert.Equal("name", column.Name);
        Assert.Null(column.Qualifier);
    }

    [Fact]
    public void Fields_Creates_Qualified_Columns_From_Relations()
    {
        var field = new QualifiedField("name", ColumnDataType.Integer);
        var column = field.FromQualified(new TableReference("table"));

        Assert.Equal("name", column.Name);
        Assert.NotNull(column.Qualifier);
    }
}