using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using SqlServer.DataTransfer.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

namespace AO.SqlServer
{
    public class DataTransfer
    {
        private readonly DataSet _dataSet;
        private List<TableDefinition> _tableDefs;

        private const string entryData = "data.xml";
        private const string entrySchema = "schema.xml";

        public DataTransfer()
        {
            _dataSet = new DataSet();
            _tableDefs = new List<TableDefinition>();
        }

        public async Task AddTableAsync(SqlConnection connection, string schema, string tableName, string criteria = null)
        {
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
            _tableDefs.Add(GetTableDefinition(connection, schema, tableName));
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
                foreach (var tbl in _tableDefs)
                {
                    var entry = zip.CreateEntry($"{tbl.Schema}-{tbl.Name}.json");
                    using (var entryStream = entry.Open())
                    {
                        string json = JsonConvert.SerializeObject(tbl);
                        using (var writer = new StreamWriter(entryStream))
                        {
                            await writer.WriteAsync(json);
                        }
                    }
                }

                await WriteEntryInnerAsync(zip, entryData, (stream) => _dataSet.WriteXml(stream));
                await WriteEntryInnerAsync(zip, entrySchema, (stream) => _dataSet.WriteXmlSchema(stream));
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

        private static TableDefinition GetTableDefinition(SqlConnection connection, string schema, string tableName)
        {
            throw new NotImplementedException();
        }
    }
}
