using System.Collections.Generic;

namespace SqlServer.DataTransfer.Models
{
    public enum IndexType
    {
        PrimaryKey,
        UniqueConstraint,
        Index
    }

    public class TableDefinition
    {
        public string Schema { get; set; }
        public string Name { get; set; }        
        public string IdentityColumn { get; set; }
        public List<Column> Columns { get; set; }
        public List<Index> Indexes { get; set; }

        public class Column
        {
            public string Name { get; set; }
            public string DataType { get; set; }            
            public bool IsNullable { get; set; }
        }

        public class Index
        {
            public string Name { get; set; }
            public bool IsClustered { get; set; }
            public IndexType Type { get; set; }
            public List<string> Columns { get; set; }
        }
    }
}
