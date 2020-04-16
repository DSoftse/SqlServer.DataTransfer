This came from a need to support better seed data features with my [LocalDb](https://github.com/adamosoftware/SqlServer.LocalDb) project. Originally I wanted to add support for .bacpac files so that seed data could be added easily to `LocalDb` tests. However, everything I've seen about working with .bacpac files programmatically is command-line based (e.g. [sqlpackage.exe](https://docs.microsoft.com/en-us/sql/tools/sqlpackage?view=sql-server-ver15)), and I'm just not into that. This was one of those times I saw an opportunity to write a C# library, and I ran with it.

The Nuget package is **AO.SqlServer.DataTransfer**.

Exporting data uses the `DataTransfer` object, and looks like this. In this example, I'm exporting two tables `WorkItem` and `Comment` along with some criteria for each table. In this example, I'm using `LocalDb` as the connection source, but it works with any `SqlConnection`.

```csharp
var dt = new DataTransfer();
using (var cn = LocalDb.GetConnection("your db"))
{
    await dt.AddTableAsync(source, "dbo", "WorkItem", "[OrganizationId]=1");
    await dt.AddTableAsync(source, "dbo", "Comment", "[ObjectId] IN (SELECT [Id] FROM [dbo].[WorkItem] WHERE [OrganizationId]=1)");
}

await dt.ExportAsync(@"C:\users\adam\desktop\MyExport.zip");
```
Importing data looks like this:

```csharp
using (var cn = LocalDb.GetConnection("another db"))
{
    dt = new DataTransfer();
    await dt.ImportAsync(cn, @"C:\users\adam\desktop\MyExport.zip");
}
```

The export logic is [here](https://github.com/adamosoftware/SqlServer.DataTransfer/blob/master/SqlServer.DataTransfer/DataTransfer_export.cs), and has two important methods [AddTableAsync](https://github.com/adamosoftware/SqlServer.DataTransfer/blob/master/SqlServer.DataTransfer/DataTransfer_export.cs#L29) and [ExportAsync](https://github.com/adamosoftware/SqlServer.DataTransfer/blob/master/SqlServer.DataTransfer/DataTransfer_export.cs#L77). This uses [SMO](https://docs.microsoft.com/en-us/sql/relational-databases/server-management-objects-smo/overview-smo?view=sql-server-ver15) under the hood to generate accurate [CREATE TABLE](https://github.com/adamosoftware/SqlServer.DataTransfer/blob/master/SqlServer.DataTransfer/DataTransfer_export.cs#L49) statements. I had worked a little with SMO in years past, so fortunately I wasn't starting completely from scratch. SMO is a large library with much to get lost in. Just a side note: I [omit foreign keys](https://github.com/adamosoftware/SqlServer.DataTransfer/blob/master/SqlServer.DataTransfer/DataTransfer_export.cs#L60) from my generated script so I don't have to worry about table creation order in the destination database, and FKs have no benefit for a read-only data transfer medium like this. Note also I have `ExportAsync` overloads that accept both a filename and a stream. Stream support is always a good idea because it opens up many options for where you want output to end up. Sometimes I write directly to blob storage, for example, and that is all Stream-based.

The import logic is [here](https://github.com/adamosoftware/SqlServer.DataTransfer/blob/master/SqlServer.DataTransfer/DataTransfer_import.cs), with one public method [ImportAsync](https://github.com/adamosoftware/SqlServer.DataTransfer/blob/master/SqlServer.DataTransfer/DataTransfer_import.cs#L15). Here again, I have overloads that accept both a stream and filename. Note that I'm using old school ADO.NET DataSets and their built-in XML serialization capability. The `Import` method uses [ReadXml](https://github.com/adamosoftware/SqlServer.DataTransfer/blob/master/SqlServer.DataTransfer/DataTransfer_import.cs#L27), and in the `Export` method uses [WriteXml](https://github.com/adamosoftware/SqlServer.DataTransfer/blob/master/SqlServer.DataTransfer/DataTransfer_export.cs#L91). DataSets have some schema writing ability, but I'm actually not clear what exactly it includes. I guess I could look. I use SMO for CREATE TABLE statements and [save that as json](https://github.com/adamosoftware/SqlServer.DataTransfer/blob/master/SqlServer.DataTransfer/DataTransfer_export.cs#L31) because I know those will work to create real SQL Server tables. I think the ADO.NET DataSet "schema" info is just the column metadata that's CLR-relevant, not the complete SQL DDL behind them.

## About the Testing project
I wrote an [integration test](https://github.com/adamosoftware/SqlServer.DataTransfer/blob/master/Testing/DataTransferTests.cs) using a local database I happen to have personally. If you clone the repo and run it as-is, it won't work. This was indeed one of the motivations for this project, so that on other projects I could more easily bundle seed data to make them more portable.
