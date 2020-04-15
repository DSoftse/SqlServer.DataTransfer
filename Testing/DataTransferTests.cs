using AO.SqlServer;
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

            using (var cn = LocalDb.GetConnection("Ginseng8"))
            {
                dt.AddTableAsync(cn, "dbo", "WorkItem", "[OrganizationId]=1").Wait();
            }

            dt.SaveAsync(@"C:\users\adam\desktop\Ginseng.zip").Wait();
        }
    }
}
