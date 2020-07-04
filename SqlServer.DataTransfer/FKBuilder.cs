using Dapper;
using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AO.SqlServer
{
    public class FKInfoResult
    {
        public int ObjectId { get; set; }
        public string ConstraintName { get; set; }
        public byte? CascadeDelete { get; set; }
        public byte? CascadeUpdate { get; set; }
        public string ReferencingSchema { get; set; }
        public string ReferencingTable { get; set; }
        public string ReferencingColumn { get; set; }
        public string ReferencedColumn { get; set; }
        public string ReferencedSchema { get; set; }
        public string ReferencedTable { get; set; }
        public int ReferencingObjectId { get; set; }
        public int ReferencedObjectId { get; set; }

        public bool IsCascadeDelete => CascadeDelete?.Equals(1) ?? false;
        public bool IsCascadeUpdate => CascadeUpdate?.Equals(1) ?? false;
    }

    public class FKBuilder
    {
        private async Task<IEnumerable<FKInfoResult>> GetAllFKsAsync(SqlConnection connection) =>
            await connection.QueryAsync<FKInfoResult>(
                @"SELECT 
                    [fk].[object_id] AS [ObjectId],
                    [fk].[name] AS [ConstraintName],
                    [fk].[delete_referential_action] AS [CascadeDelete],
                    [fk].[update_referential_action] AS [CascadeUpdate],
                    SCHEMA_NAME([referencing_table].[schema_id]) AS [ReferencingSchema],
                    [referencing_table].[name] AS [ReferencingTable],
                    [referencing_col].[name] AS [ReferencingColumn],
                    [referenced_col].[name] AS [ReferencedColumn],
                    SCHEMA_NAME([referenced_table].[schema_id]) AS [ReferencedSchema],
                    [referenced_table].[name] AS [ReferencedTable],
                    [referencing_table].[object_id] AS [ReferencingObjectId],
                    [referenced_table].[object_id] AS [ReferencedObjectId]
                FROM 
                    [sys].[foreign_keys] [fk]
                    INNER JOIN [sys].[foreign_key_columns] [fk_col] ON [fk].[object_id]=[fk_col].[constraint_object_id]
                    INNER JOIN [sys].[all_columns] [referencing_col] ON 
                        [fk_col].[parent_column_id]=[referencing_col].[column_id] AND
                        [fk_col].[parent_object_id]=[referencing_col].[object_id]
                    INNER JOIN [sys].[all_columns] [referenced_col] ON
                        [fk_col].[referenced_column_id]=[referenced_col].[column_id] AND
                        [fk_col].[referenced_object_id]=[referenced_col].[object_id]
                    INNER JOIN [sys].[tables] [referenced_table] ON [referenced_col].[object_id]=[referenced_table].[object_id]
                    INNER JOIN [sys].[tables] [referencing_table] ON [referencing_col].[object_id]=[referencing_table].[object_id]");

        public async Task<IEnumerable<string>> GetForeignKeysAsync(SqlConnection connection, HashSet<int> objectIds)
        {
            var allFKs = await GetAllFKsAsync(connection);

            var filteredFKs = allFKs
                .Where(row => objectIds.Contains(row.ReferencedObjectId) && objectIds.Contains(row.ReferencingObjectId))
                .ToLookup(row => row.ObjectId);

            return filteredFKs.Select(fk => GetFKSyntax(fk));
        }

        private string GetFKSyntax(IGrouping<int, FKInfoResult> fkColumns)
        {
            var fk = fkColumns.First();

            string referencingColumns = string.Join(", ", fkColumns.Select(col => $"[{col.ReferencingColumn}]"));
            string referencedColumns = string.Join(", ", fkColumns.Select(col => $"[{col.ReferencedColumn}]"));

            string result = $"ALTER TABLE [{fk.ReferencedSchema}].[{fk.ReferencedTable}] ADD CONSTRAINT [{fk.ConstraintName}] FOREIGN KEY ({referencingColumns}) REFERENCES [{fk.ReferencedSchema}].[{fk.ReferencedTable}] ({referencedColumns})";

            if (fk.IsCascadeDelete) result += " ON DELETE CASCADE";
            if (fk.IsCascadeUpdate) result += " ON UPDATE CASCADE";

            return result;
        }
    }
}
