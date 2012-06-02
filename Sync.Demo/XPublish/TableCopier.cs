using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace DXP.Adam.Recovery.XPublish {
    internal class TableCopier {
        private readonly string _backupConnectionString;

        public TableCopier(string backupConnectionString) {
            _backupConnectionString = backupConnectionString;
        }

        private static SqlBulkCopy CreateBulkCopy(SqlTransaction transaction) {
            var bulkCopy = new SqlBulkCopy(transaction.Connection, SqlBulkCopyOptions.KeepIdentity, transaction) {
                BatchSize = 10

            };

            return bulkCopy;
        }

        private static void PrepareBulkCopy(SqlBulkCopy bulkCopy, string tableName) {
            bulkCopy.ColumnMappings.Clear();
            bulkCopy.DestinationTableName = string.Format("[{0}]", tableName);

            var columnList = ColumnMapper.Instance[tableName];
            foreach (var column in columnList) {
                var formalColumn = string.Format("[{0}]", column);
                bulkCopy.ColumnMappings.Add(formalColumn, formalColumn);
            }
        }

        private static void CopyTable(SqlBulkCopy bulkCopy, SqlCommand command, string tableName) {
            PrepareBulkCopy(bulkCopy, tableName);

            using (var reader = command.ExecuteReader()) {
                if (reader != null) {
                    bulkCopy.WriteToServer(reader);
                    reader.Close();
                }
            }
        }

        private void CopyTable(SqlBulkCopy bulkCopy, string filterColumn, IEnumerable<Guid> filterIds, string tableName) {
            var command = CommandBuilder.PrepareCommand(tableName, filterColumn, filterIds, SqlBuilder.BuildSelectSql);

            using (var backupConnection = new SqlConnection(_backupConnectionString)) {
                backupConnection.Open();
                command.Connection = backupConnection;

                CopyTable(bulkCopy, command, tableName);
            }
        }

        public void DoCopy(SqlTransaction transaction, string filterColumn, IEnumerable<Guid> filterIds, params string[] tableNames) {
            using (var bulkCopy = CreateBulkCopy(transaction)) {
                foreach (var tableName in tableNames) {
                    CopyTable(bulkCopy, filterColumn, filterIds, tableName);
                }
            }
        }
    }
}
