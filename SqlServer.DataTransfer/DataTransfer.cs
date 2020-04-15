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
    public class DataTransfer
    {
        private readonly DataSet _dataSet;
        private Dictionary<string, List<string>> _createTables;

        private const string entryData = "data.xml";
        private const string entrySchema = "schema.json";

        public DataTransfer()
        {
            _dataSet = new DataSet();
            _createTables = new Dictionary<string, List<string>>();
        }

        public async Task AddTableAsync(SqlConnection connection, string schema, string tableName, string criteria = null)
        {
            _createTables.Add($"{schema}-{tableName}", CreateTableStatement(connection, schema, tableName));

            DataTable dataTable = new DataTable($"{schema}-{tableName}");

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

        public async Task SaveAsync(string fileName)
        {
            using (var output = File.Create(fileName))
            {
                await SaveAsync(output);
            }
        }

        public async Task SaveAsync(Stream output)
        {
            using (var zip = new ZipArchive(output, ZipArchiveMode.Create))
            {
                var entry = zip.CreateEntry(entrySchema);
                string json = JsonConvert.SerializeObject(_createTables);
                using (var entryStream = entry.Open())
                {                        
                    using (var writer = new StreamWriter(entryStream))
                    {
                        await writer.WriteAsync(json);
                    }                    
                }

                await WriteEntryInnerAsync(zip, entryData, (stream) => _dataSet.WriteXml(stream));                
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
    }
}
