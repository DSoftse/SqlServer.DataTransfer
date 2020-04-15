using AO.SqlServer;
using Dapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlServer.LocalDb;

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

            const string fileName = @"C:\users\adam\desktop\Ginseng.zip";            
            dt.ExportAsync(fileName).Wait();

            dt = new DataTransfer();
            using (var dest = LocalDb.GetConnection("DataTransfer"))
            {
                // make sure clean slate
                try { dest.Execute("DROP TABLE [dbo].[WorkItem]"); } catch { /* do nothing */ }
                try { dest.Execute("DROP TABLE [dbo].[Comment]"); } catch { /* do nothing */ }

                dt.ImportAsync(dest, fileName).Wait();

                Assert.IsTrue(dt["dbo.WorkItem"].Equals(workItemRecords));
                Assert.IsTrue(dt["dbo.Comment"].Equals(commentRecords));
            }
        }
    }
}
