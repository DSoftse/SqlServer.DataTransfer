using AO.SqlServer;
using AO.SqlServer.Models;
using Dapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlServer.LocalDb;
using System;
using System.IO;
using System.Linq;

namespace Testing
{
    [TestClass]
    public class DataTransferTests
    {
        [TestMethod]
        public void CreateGinsengZip()
        {
            var dt = new DataTransfer();

            int workItemRecords = 0;
            int commentRecords = 0;

            using (var source = LocalDb.GetConnection("Ginseng8"))
            {
                dt.AddTableAsync(source, "dbo", "WorkItem", "[OrganizationId]=1").Wait();
                dt.AddTableAsync(source, "dbo", "Comment", "[ObjectId] IN (SELECT [Id] FROM [dbo].[WorkItem] WHERE [OrganizationId]=1)").Wait();

                workItemRecords = dt["dbo.WorkItem"];
                commentRecords = dt["dbo.Comment"];
            }

            string fileName = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "GinsengSelectTables.zip");
            dt.ExportAsync(fileName).Wait();

            dt = new DataTransfer();
            using (var dest = LocalDb.GetConnection("DataTransfer"))
            {
                // make sure clean slate
                try { dest.Execute("DROP TABLE [dbo].[WorkItem]"); } catch { /* do nothing */ }
                try { dest.Execute("DROP TABLE [dbo].[Comment]"); } catch { /* do nothing */ }

                dt.ImportFileAsync(dest, fileName).Wait();

                Assert.IsTrue(dt["dbo.WorkItem"].Equals(workItemRecords));
                Assert.IsTrue(dt["dbo.Comment"].Equals(commentRecords));
            }
        }

        [TestMethod]
        public void CreateGinsengZipAllTables()
        {
            var dt = new DataTransfer();
            using (var source = LocalDb.GetConnection("Ginseng8"))
            {
                dt.AddAllTablesAsync(source).Wait();
            }

            string fileName = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "GinsengAllTables.zip");            
            dt.ExportAsync(fileName).Wait();

            dt = new DataTransfer();
            using (var dest = LocalDb.GetConnection("DataTransfer"))
            {
                // make sure clean slate
                var tables = dest.QueryAsync<ObjectName>("SELECT SCHEMA_NAME([schema_id]) AS [Schema], [name] AS [Name] FROM [sys].[tables]").Result;
                foreach (var tbl in tables)
                {
                    try { dest.Execute($"DROP TABLE [{tbl.Schema}].[{tbl.Name}]"); } catch { /* do nothing */ }
                }

                foreach (var schemaGrp in tables.GroupBy(item => item.Schema).Where(grp => !grp.Key.ToLower().Equals("dbo")))
                {
                    try { dest.Execute($"DROP SCHEMA [{schemaGrp.Key}]"); } catch { /* do nothing */ }
                }

                dt.ImportFileAsync(dest, fileName).Wait();
            }
        }
    }
}
