using Dapper;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
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
        public async Task ImportAsync(SqlConnection connection, Stream input)
        {
            using (var zip = new ZipArchive(input, ZipArchiveMode.Read))
            {
                var allTables = GetSchema(zip);
                var createdTables = await CreateTablesIfNotExistsAsync(connection, allTables);
                
                var entry = zip.GetEntry(entryData);
                using (var stream = entry.Open())
                {
                    await Task.Run(() =>
                    {
                        _dataSet.ReadXml(stream);
                        MarkAddedRows(_dataSet);
                        foreach (var tableName in createdTables) InsertRows(connection, tableName);
                    });
                }                
            }
        }

        public async Task ImportAsync(SqlConnection connection, string fileName)
        {
            using (var stream = File.OpenRead(fileName))
            {
                await ImportAsync(connection, stream);
            }
        }

        private void InsertRows(SqlConnection connection, string tableName)
        {
            var dataTable = _dataSet.Tables[tableName];
            var name = ParseTableName(tableName);

            using (SqlCommand select = BuildSelectCommand(dataTable, connection, name.Item1, name.Item2))
            {
                using (var adapter = new SqlDataAdapter(select))
                {
                    using (var builder = new SqlCommandBuilder(adapter))
                    {
                        adapter.InsertCommand = builder.GetInsertCommand();
                        adapter.Update(dataTable);
                    }
                }
            }
        }

        private SqlCommand BuildSelectCommand(DataTable table, SqlConnection connection, string schemaName, string tableName)
        {
            string[] columnNames = table.Columns.OfType<DataColumn>().Select(col => col.ColumnName).ToArray();
            string query = $"SELECT {string.Join(", ", columnNames.Select(col => $"[{col}]"))} FROM [{schemaName}].[{tableName}]";
            return new SqlCommand(query, connection);
        }

        /// <summary>
        /// this is needed so that adapters properly insert rows
        /// </summary>
        private static void MarkAddedRows(DataSet dataSet)
        {
            foreach (DataTable tbl in dataSet.Tables)
            {
                foreach (DataRow row in tbl.Rows)
                {
                    row.AcceptChanges();
                    row.SetAdded();
                }
            }
        }

        private static (string, string) ParseTableName(string tableName)
        {
            var parts = tableName.Split('.');
            return (parts.Length > 1) ? (parts[0], parts[1]) : ("dbo", parts[0]);
        }

        private async Task<IEnumerable<string>> CreateTablesIfNotExistsAsync(SqlConnection connection, Dictionary<string, List<string>> createTables)
        {
            List<string> newTables = new List<string>();

            async Task<bool> tableExistsAsync(string tableName)
            {
                var parts = ParseTableName(tableName);
                return (await connection.QuerySingleOrDefaultAsync<int>(
                    "SELECT 1 FROM [sys].[tables] WHERE SCHEMA_NAME([schema_id])=@schema AND [name]=@name",
                    new { schema = parts.Item1, name = parts.Item2 })) == 1;
            };

            foreach (var commands in createTables)
            {
                if (!await tableExistsAsync(commands.Key))
                {
                    newTables.Add(commands.Key);
                    foreach (var cmd in commands.Value) await connection.ExecuteAsync(cmd);
                }
            }

            return newTables;
        }

        private Dictionary<string, List<string>> GetSchema(ZipArchive zip)
        {
            var entry = zip.GetEntry(entrySchema);
            using (var stream = entry.Open())
            {
                using (var reader = new StreamReader(stream))
                {
                    string json = reader.ReadToEnd();
                    return JsonConvert.DeserializeObject<Dictionary<string, List<string>>>(json);
                }
            }
        }
    }
}
