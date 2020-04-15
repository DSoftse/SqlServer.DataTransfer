using System.Collections.Generic;

namespace SqlServer.DataTransfer.Models
{
    public class SqlTable
    {
        public TableDefinition Definition { get; set; }
        public List<Dictionary<string, string>> Rows { get; set; }
    }
}
