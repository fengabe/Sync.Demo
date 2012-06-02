using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace DXP.Adam.Recovery.XPublish {
    internal class TemplateRestorer : Restorer {
        private const string FilterColumn = "AdamRecordId";

        public TemplateRestorer(string liveConnectionString, string backupConnectionString)
            : base(liveConnectionString, backupConnectionString) {
        }

        public static void DoDelete(SqlTransaction transaction, IEnumerable<Guid> templateIds) {
            if (templateIds == null || templateIds.Count() == 0) {
                return;
            }

            TableDeleter.DoDelete(transaction, FilterColumn, templateIds,
                         TableName.CommentEntry,
                         TableName.OrderUserGroups,
                         TableName.Value,
                         TableName.Ad,
                         TableName.Field,
                         TableName.XPublishTemplate);
        }

        private void DoInsert(SqlTransaction transaction, IEnumerable<Guid> templateIds) {
            if (templateIds == null || templateIds.Count() == 0) {
                return;
            }

            TableCopier.DoCopy(transaction, FilterColumn,
                templateIds,
                TableName.XPublishTemplate,
                TableName.Field);
        }

        private void DoUpdate(SqlTransaction transaction, IEnumerable<Guid> templateIds) {
            if (templateIds == null || templateIds.Count() == 0) {
                return;
            }

            TableUpdater.DoUpdate(transaction, FilterColumn,
                templateIds,
                TableName.XPublishTemplate,
                TableName.Field);
        }

        public void RestoreWithAds(IEnumerable<Guid> templateIds) {
            if (templateIds == null || templateIds.Count() == 0) {
                return;
            }

            using (var connection = new SqlConnection(LiveConnectionString)) {
                connection.Open();

                using (var transaction = connection.BeginTransaction()) {
                    TableDeleter.DoDelete(transaction, FilterColumn,
                        templateIds,
                        TableName.CommentEntry,
                        TableName.OrderUserGroups,
                        TableName.Value,
                        TableName.Ad,
                        TableName.Field,
                        TableName.XPublishTemplate);

                    TableCopier.DoCopy(transaction, FilterColumn,
                        templateIds,
                        TableName.XPublishTemplate,
                        TableName.Field,
                        TableName.Ad,
                        TableName.Value,
                        TableName.CommentEntry,
                        TableName.OrderUserGroups);

                    transaction.Commit();
                }
            }
        }

        public void Restore(IEnumerable<Guid> templateIds) {
            if (templateIds == null || templateIds.Count() == 0) {
                return;
            }

            var liveTemplateIds = SelectAdamRecordIds(LiveConnectionString, TableName.XPublishTemplate, templateIds);
            var backupTemplateIds = SelectAdamRecordIds(BackupConnectionString, TableName.XPublishTemplate, templateIds);

            var toInsertTemplateIds = backupTemplateIds.Except(liveTemplateIds);
            var toDeleteTemplateIds = liveTemplateIds.Except(backupTemplateIds);
            var toUpdateTemplateIds = backupTemplateIds.Intersect(liveTemplateIds);

            using (var connection = new SqlConnection(LiveConnectionString)) {
                connection.Open();

                using (var transaction = connection.BeginTransaction()) {
                    DoDelete(transaction, toDeleteTemplateIds);
                    DoInsert(transaction, toInsertTemplateIds);
                    DoUpdate(transaction, toUpdateTemplateIds);

                    transaction.Commit();
                }
            }
        }
    }
}
