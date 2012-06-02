using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace DXP.Adam.Recovery.XPublish {
    internal class TableDeleter {
        private static void Delete(SqlTransaction transaction, string tableName, string filterColumn, IEnumerable<Guid> filterIds) {
            var command = CommandBuilder.PrepareCommand(tableName, filterColumn, filterIds, SqlBuilder.BuildDeleteSql);
            command.Connection = transaction.Connection;
            command.Transaction = transaction;

            command.ExecuteNonQuery();
        }

        public static void DoDelete(SqlTransaction transaction, string filterColumn, IEnumerable<Guid> filterIds, params string[] tableNames) {
            foreach (var tableName in tableNames) {
                Delete(transaction, tableName, filterColumn, filterIds);
            }
        }
    }
}
