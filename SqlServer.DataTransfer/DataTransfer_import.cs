using AO.SqlServer.Models;
using Dapper;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace AO.SqlServer
{
    public partial class DataTransfer
    {
        public async Task ImportAsync(SqlConnection connection, Stream input)
        {
            using (var zip = new ZipArchive(input, ZipArchiveMode.Read))
            {
                var allTables = DeserializeEntryJson<Dictionary<string, List<string>>>(zip, entrySchema);
                var createdTables = await CreateTablesIfNotExistsAsync(connection, allTables);
                
                var entry = zip.GetEntry(entryData);
                using (var stream = entry.Open())
                {
                    await Task.Run(async () =>
                    {
                        _dataSet.ReadXml(stream);
                        MarkAddedRows(_dataSet);
                        foreach (var tableName in createdTables) await InsertRows(connection, tableName);
                    });
                }

                var foreignKeys = DeserializeEntryJson<List<FKBuilder.ConstraintObject>>(zip, entryFKs);
                foreach (var fk in foreignKeys)
                {
                    if (!await foreignKeyExists(fk.Name))
                    {
                        await connection.ExecuteAsync(fk.Command);
                    }
                }
            }

            async Task<bool> foreignKeyExists(string name) => (await connection.QuerySingleOrDefaultAsync<int>("SELECT 1 FROM [sys].[foreign_keys] WHERE [name]=@name", new { name })).Equals(1);
        }

        public async Task ImportFileAsync(SqlConnection connection, string fileName)
        {
            using (var stream = File.OpenRead(fileName))
            {
                await ImportAsync(connection, stream);
            }
        }        

        public async Task ImportResourceAsync(SqlConnection connection, string resourceName)
        {
            using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(resourceName))
            {
                await ImportAsync(connection, stream);
            }
        }

        private async Task InsertRows(SqlConnection connection, string tableName)
        {
            var dataTable = _dataSet.Tables[tableName];
            var name = ParseTableName(tableName);

            bool identityIns = await hasIdentity(name);

            try
            {
                if (identityIns) connection.Execute($"SET IDENTITY_INSERT [{name.Schema}].[{name.Name}] ON");

                using (SqlCommand select = BuildSelectCommand(dataTable, connection, name.Schema, name.Name))
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
            finally
            {
                if (identityIns) connection.Execute($"SET IDENTITY_INSERT [{name.Schema}].[{name.Name}] OFF");                
            }

            async Task<bool> hasIdentity(ObjectName objectName)
            {
                int objectId = await GetObjectId(connection, objectName.Schema, objectName.Name);
                return (await connection.QueryAsync<int>("SELECT [is_identity] FROM [sys].[columns] WHERE [object_id]=@objectId", new { objectId })).Any(value => value == 1);
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

        private static ObjectName ParseTableName(string tableName)
        {
            var parts = tableName.Split('.');
            return (parts.Length > 1) ? 
                new ObjectName() { Schema = parts[0], Name = parts[1] } : 
                new ObjectName() { Schema = "dbo", Name = parts[0] };
        }

        private async Task<IEnumerable<string>> CreateTablesIfNotExistsAsync(SqlConnection connection, Dictionary<string, List<string>> createTables)
        {
            List<string> newTables = new List<string>();

            async Task<bool> tableExistsAsync(string tableName)
            {
                var parts = ParseTableName(tableName);
                return (await connection.QuerySingleOrDefaultAsync<int>(
                    "SELECT 1 FROM [sys].[tables] WHERE SCHEMA_NAME([schema_id])=@schema AND [name]=@name",
                    new { schema = parts.Schema, name = parts.Name })) == 1;
            };

            async Task<bool> schemaExistsAsync(string schemaName)
            {
                return (await connection.QuerySingleOrDefaultAsync<int>(
                    "SELECT 1 FROM [sys].[schemas] WHERE [name]=@name", 
                    new { name = schemaName }) == 1);
            }

            foreach (var commands in createTables)
            {
                string schema = ParseTableName(commands.Key).Schema;
                if (!await schemaExistsAsync(schema))
                {
                    await connection.ExecuteAsync($"CREATE SCHEMA [{schema}]");
                }

                if (!await tableExistsAsync(commands.Key))
                {
                    newTables.Add(commands.Key);
                    foreach (var cmd in commands.Value) await connection.ExecuteAsync(cmd);
                }
            }

            return newTables;
        }

        private T DeserializeEntryJson<T>(ZipArchive zip, string entryName)
        {
            var entry = zip.GetEntry(entryName);
            if (entry != null)
            {
                using (var stream = entry.Open())
                {
                    using (var reader = new StreamReader(stream))
                    {
                        string json = reader.ReadToEnd();
                        return JsonConvert.DeserializeObject<T>(json);
                    }
                }
            }

            return default;
        }
    }
}
