using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace DXP.Adam.Recovery.XPublish {
    internal class TableUpdater {
        private readonly string _backupConnectionString;

        public TableUpdater(string backupConnectionString) {
            _backupConnectionString = backupConnectionString;
        }

        private static List<SqlParameter> CreateParameters(IDataRecord reader, string tableName) {
            var columns = ColumnMapper.Instance[tableName];
            var parameters = new List<SqlParameter>();

            foreach (string column in columns) {
                var parameter = new SqlParameter(column, reader[column]);
                parameters.Add(parameter);
            }

            return parameters;
        }

        private static void UpdateRow(SqlTransaction transaction, IDataRecord reader, string tableName) {
            var parameters = CreateParameters(reader, tableName);
            string sql = SqlBuilder.BuildUpdateSql(tableName);

            var updateCommand = CommandBuilder.CreateCommand(sql, parameters);
            updateCommand.Transaction = transaction;
            updateCommand.Connection = transaction.Connection;

            updateCommand.ExecuteNonQuery();
        }

        private static void Update(SqlTransaction transaction, SqlCommand command, string tableName) {
            using (var reader = command.ExecuteReader()) {
                if (reader != null) {
                    while (reader.Read()) {
                        UpdateRow(transaction, reader, tableName);
                    }

                    reader.Close();
                }
            }
        }

        private void Update(SqlTransaction transaction, string filterColumn, IEnumerable<Guid> filterIds, string tableName) {
            var command = CommandBuilder.PrepareCommand(tableName, filterColumn, filterIds, SqlBuilder.BuildSelectSql);
            using (var backupConnection = new SqlConnection(_backupConnectionString)) {
                backupConnection.Open();
                command.Connection = backupConnection;

                Update(transaction, command, tableName);
            }
        }

        public void DoUpdate(SqlTransaction transaction, string filterColumn, IEnumerable<Guid> filterIds, params string[] tableNames) {
            foreach (var tableName in tableNames) {
                Update(transaction, filterColumn, filterIds, tableName);
            }
        }
    }
}
