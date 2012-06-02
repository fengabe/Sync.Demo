using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace DXP.Adam.Recovery.XPublish {
    internal class MasterRestorer : Restorer {
        private const string FilterColumn = "AdamRecordId";

        public MasterRestorer(string liveConnectionString, string backupConnectionString)
            : base(liveConnectionString, backupConnectionString) {
        }

        private IEnumerable<Guid> SelectTemplateIdsFromMasters(IEnumerable<Guid> masterIds) {
            string sql = SqlBuilder.GetTemplatesFromMastersSql(FilterColumn, masterIds.Count());

            var parameters = CommandBuilder.CreateParameters(FilterColumn, masterIds);
            var command = CommandBuilder.CreateCommand(sql, parameters);

            var recordIds = new List<Guid>();

            using (var connection = new SqlConnection(LiveConnectionString)) {
                connection.Open();
                command.Connection = connection;

                using (var reader = command.ExecuteReader()) {
                    if (reader != null) {
                        while (reader.Read()) {
                            var ordinal = reader.GetOrdinal(FilterColumn);
                            var recordId = reader.GetGuid(ordinal);
                            recordIds.Add(recordId);
                        }
                    }
                }
            }

            return recordIds;
        }

        private void DoDelete(SqlTransaction transaction, IEnumerable<Guid> masterIds) {
            if (masterIds == null || masterIds.Count() == 0) {
                return;
            }

            var toDeleteTemplateIds = SelectTemplateIdsFromMasters(masterIds);
            TemplateRestorer.DoDelete(transaction, toDeleteTemplateIds);

            TableDeleter.DoDelete(transaction, FilterColumn, masterIds,
                 TableName.TextColor,
                 TableName.CharacterStyles,
                 TableName.Tag,
                 TableName.TagGroup,
                 TableName.Master);
        }

        private void DoInsert(SqlTransaction transaction, IEnumerable<Guid> masterIds) {
            if (masterIds == null || masterIds.Count() == 0) {
                return;
            }

            TableCopier.DoCopy(transaction, FilterColumn,
                masterIds,
                TableName.Master,
                TableName.CharacterStyles,
                TableName.TextColor,
                TableName.TagGroup,
                TableName.Tag
                );
        }

        private void DoUpdate(SqlTransaction transaction, IEnumerable<Guid> masterIds) {
            if (masterIds == null || masterIds.Count() == 0) {
                return;
            }

            TableDeleter.DoDelete(transaction, FilterColumn,
                masterIds,
                TableName.TextColor,
                TableName.CharacterStyles);

            TableCopier.DoCopy(transaction, FilterColumn,
                masterIds,
                TableName.TextColor,
                TableName.CharacterStyles);

            TableUpdater.DoUpdate(transaction, FilterColumn,
                masterIds,
                TableName.Master,
                TableName.TagGroup,
                TableName.Tag
            );
        }

        public void Restore(IEnumerable<Guid> masterIds) {
            if (masterIds == null || masterIds.Count() == 0) {
                return;
            }

            var liveMasterIds = SelectAdamRecordIds(LiveConnectionString, TableName.Master, masterIds);
            var backupMasterIds = SelectAdamRecordIds(BackupConnectionString, TableName.Master, masterIds);

            var toDeleteMasterIds = liveMasterIds.Except(backupMasterIds);
            var toInsertMasterIds = backupMasterIds.Except(liveMasterIds);
            var toUpdateMasterIds = liveMasterIds.Intersect(backupMasterIds);

            using (var connection = new SqlConnection(LiveConnectionString)) {
                connection.Open();

                using (var transaction = connection.BeginTransaction()) {
                    DoDelete(transaction, toDeleteMasterIds);
                    DoInsert(transaction, toInsertMasterIds);
                    DoUpdate(transaction, toUpdateMasterIds);

                    transaction.Commit();
                }
            }
        }
    }
}
