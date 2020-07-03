using Dapper;
using Microsoft.Data.SqlClient;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;

namespace AO.SqlServer
{
    public static class TableOrder
    {
        public static async Task<IEnumerable<TableInfo>> GetCreateOrderAsync(SqlConnection connection)
        {
            throw new NotImplementedException();

            var result = new List<TableInfo>();
            result.AddRange(await getRootTables());

            var remainingTables = (await getRemainingTables()).ToList();

            int level = 0;
            while (remainingTables.Any())
            {
                level++;
                foreach (var tbl in remainingTables)
                {
                }
            }

            async Task<IEnumerable<TableInfo>> getRootTables()
            {
                return await connection.QueryAsync<TableInfo>(
                    @"SELECT 
	                    SCHEMA_NAME([t].[schema_id]) AS [schema],
	                    [t].[name],
	                    [t].[object_id]
                    FROM 
	                    [sys].[tables] [t]
                    WHERE 
	                    NOT EXISTS(SELECT 1 FROM [sys].[foreign_keys] WHERE [parent_object_id]=[t].[object_id]) AND
	                    [t].[type_desc]='USER_TABLE'");
            }

            async Task<IEnumerable<TableInfo>> getRemainingTables()
            {
                return await connection.QueryAsync<TableInfo>(
                    @"SELECT 
	                    SCHEMA_NAME([t].[schema_id]) AS [schema],
	                    [t].[name],
	                    [t].[object_id]
                    FROM 
	                    [sys].[tables] [t]
                    WHERE 
	                    EXISTS(SELECT 1 FROM [sys].[foreign_keys] WHERE [parent_object_id]=[t].[object_id]) AND
	                    [t].[type_desc]='USER_TABLE'");
            }

            async Task<IEnumerable<TableInfo>> getParentTables(int objectId)
            {
                return await connection.QueryAsync<TableInfo>(
                    @"SELECT 
	                    SCHEMA_NAME([t].[schema_id]) AS [schema],
	                    [t].[name],
	                    [t].[object_id]
                    FROM 
	                    [sys].[tables] [t]
                    WHERE 
	                    EXISTS(SELECT 1 FROM [sys].[foreign_keys] WHERE [parent_object_id]=[t].[object_id]) AND
	                    [t].[type_desc]='USER_TABLE'");
            }

        }

        private static async Task<IEnumerable<FKMap>> GetFKMapAsync(SqlConnection connection) => 
            await connection.QueryAsync<FKMap>(
                @"WITH [source] AS (
	                SELECT 
		                [fk].[name],
		                [referencing_table].[name] AS [ReferencingTable],
		                [referencing_col].[name] AS [ReferencingName],
		                [referenced_col].[name] AS [ReferencedName],
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
		                INNER JOIN [sys].[tables] [referencing_table] ON [referencing_col].[object_id]=[referencing_table].[object_id]
                    ) SELECT 
	                    [ReferencedObjectId] AS [ChildId], 
	                    [ReferencingObjectId] AS [ParentId]
                    FROM 
	                    [source] 
                    GROUP BY 
	                    [ReferencedObjectId], [ReferencingObjectId]");

        private class FKMap
        {
            public int ChildId { get; set; }
            public int ParentId { get; set; }
        }
    }

    public class TableInfo
    {
        public string Schema { get; set; }
        public string Name { get; set; }
        public int ObjectId { get; set; }
        public int Level { get; set; }

        public override bool Equals(object obj)
        {
            var test = obj as TableInfo;
            return (test != null) ? test.ObjectId == ObjectId : false;
        }

        public override int GetHashCode()
        {
            return ObjectId.GetHashCode();
        }
    }

}
