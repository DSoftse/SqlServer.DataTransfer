namespace AO.SqlServer.Models
{
    public struct ObjectName
    {
        public string Schema { get; set; }
        public string Name { get; set; }
        public int ObjectId { get; set; }
    }
}
