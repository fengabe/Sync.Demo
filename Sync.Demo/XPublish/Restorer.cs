using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace DXP.Adam.Recovery.XPublish {
    internal abstract class Restorer {
        protected Restorer(string liveConnectionString, string backupConnectionString) {
            LiveConnectionString = liveConnectionString;
            BackupConnectionString = backupConnectionString;

            TableUpdater = new TableUpdater(BackupConnectionString);
            TableCopier = new TableCopier(BackupConnectionString);
        }

        public string LiveConnectionString {
            get;
            private set;
        }

        public string BackupConnectionString {
            get;
            private set;
        }

        protected TableUpdater TableUpdater {
            get;
            private set;
        }

        protected TableCopier TableCopier {
            get;
            private set;
        }

        public static IEnumerable<Guid> SelectAdamRecordIds(string connectionString, string tableName, IEnumerable<Guid> adamRecordIds) {
            const string filterColumn = "AdamRecordId";
            var command = CommandBuilder.PrepareCommand(tableName, filterColumn, adamRecordIds, SqlBuilder.BuildSelectSql);
            var recordIds = new List<Guid>();

            using (var connection = new SqlConnection(connectionString)) {
                connection.Open();
                command.Connection = connection;

                using (var reader = command.ExecuteReader()) {
                    if (reader != null) {
                        while (reader.Read()) {
                            var ordinal = reader.GetOrdinal(filterColumn);
                            var recordId = reader.GetGuid(ordinal);
                            recordIds.Add(recordId);
                        }
                    }
                }
            }

            return recordIds;
        }
    }
}
