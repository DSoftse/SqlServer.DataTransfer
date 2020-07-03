using AO.SqlServer.Models;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;

namespace AO.SqlServer
{
    public partial class DataTransfer
    {
        private readonly DataSet _dataSet;
        private Dictionary<string, List<string>> _createTables;
        private HashSet<int> _objectIds = new HashSet<int>();        

        private const string entryData = "data.xml";
        private const string entrySchema = "schema.json";        

        public DataTransfer()
        {
            _dataSet = new DataSet();
            _createTables = new Dictionary<string, List<string>>();
        }

        public async Task AddTableAsync(SqlConnection connection, string schema, string tableName, string criteria = null)
        {
            _createTables.Add($"{schema}.{tableName}", CreateTableStatement(connection, schema, tableName)); /* #createTables */
            _objectIds.Add(await getObjectId());

            DataTable dataTable = new DataTable($"{schema}.{tableName}");

            using (var cmd = BuildSelectCommand(connection, schema, tableName, criteria))
            {
                using (var adapter = new SqlDataAdapter(cmd))
                {
                    await Task.Run(() =>
                    {
                        adapter.Fill(dataTable);
                    });
                }
            }

            _dataSet.Tables.Add(dataTable);

            async Task<int> getObjectId() => 
                await connection.QuerySingleAsync<int>(
                "SELECT [object_id] FROM [sys].[tables] WHERE SCHEMA_NAME([schema_id])=@schema AND [name]=@tableName",
                new { schema, tableName });            
        }

        public async Task AddAllTablesAsync(SqlConnection connection, Func<ObjectName, bool> filter = null)
        {
            var tables = await connection.QueryAsync<ObjectName>(
                "SELECT SCHEMA_NAME([schema_id]) AS [Schema], [name] AS [Name], [object_id] AS [ObjectId] FROM [sys].[tables]");            

            foreach (var tbl in tables)
            {
                if (filter?.Invoke(tbl) ?? true) await AddTableAsync(connection, tbl.Schema, tbl.Name);
            }
        }

        private List<string> CreateTableStatement(SqlConnection connection, string schema, string tableName)
        {
            var sc = new ServerConnection(connection);
            var server = new Server(sc);
            var db = server.Databases[sc.CurrentDatabase];
            var table = db.Tables[tableName, schema];
            var scripter = new Scripter()
            {
                Server = server,
                Options = new ScriptingOptions()
                {
                    DriForeignKeys = false,
                    Indexes = true,
                    DriPrimaryKey = true
                }
            };
            var result = scripter.Script(new SqlSmoObject[] { table });
            return result.OfType<string>().ToList();
        }

        public async Task ExportAsync(string fileName)
        {
            using (var output = File.Create(fileName))
            {
                await ExportAsync(output);
            }
        }

        public async Task ExportAsync(Stream output)
        {
            using (var zip = new ZipArchive(output, ZipArchiveMode.Create))
            {
                await WriteEntryInnerAsync(zip, entrySchema, (stream) =>
                {
                    string json = JsonConvert.SerializeObject(_createTables);
                    using (var writer = new StreamWriter(stream))
                    {
                        writer.Write(json);
                    }
                });
                
                await WriteEntryInnerAsync(zip, entryData, (stream) => _dataSet.WriteXml(stream, XmlWriteMode.WriteSchema));
            }
        }

        private async Task WriteEntryInnerAsync(ZipArchive zip, string entryName, Action<Stream> action)
        {
            var entry = zip.CreateEntry(entryName);

            using (var stream = entry.Open())
            {
                await Task.Run(() =>
                {
                    action.Invoke(stream);
                });
            }
        }

        private static SqlCommand BuildSelectCommand(SqlConnection connection, string schema, string tableName, string criteria)
        {
            string query = $"SELECT * FROM [{schema}].[{tableName}]";
            if (!string.IsNullOrEmpty(criteria)) query += $" WHERE {criteria}";
            return new SqlCommand(query, connection);
        }

        public int this[string tableName]
        {
            get { return _dataSet.Tables[tableName].Rows.Count; }
        }
    }
}
