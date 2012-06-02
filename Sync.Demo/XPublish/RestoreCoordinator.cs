using System;
using System.Collections.Generic;
using System.Configuration;

namespace DXP.Adam.Recovery.XPublish {
    public class RestoreCoordinator {
        private readonly string _liveConnectionString = string.Empty;
        private readonly string _backupConnectionString = string.Empty;

        public RestoreCoordinator() {
            _liveConnectionString = ConfigurationManager.ConnectionStrings["Live"].ConnectionString;
            _backupConnectionString = ConfigurationManager.ConnectionStrings["Backup"].ConnectionString;

        }

        public void RestoreTemplates(IEnumerable<Guid> templateIds) {
            var templateRestorer = new TemplateRestorer(_liveConnectionString, _backupConnectionString);
            templateRestorer.Restore(templateIds);
        }

        public void RestoreTemplatesWithAds(IEnumerable<Guid> templateIds) {
            var templateRestorer = new TemplateRestorer(_liveConnectionString, _backupConnectionString);
            templateRestorer.RestoreWithAds(templateIds);
        }

        public void RestoreMasters(IEnumerable<Guid> masterIds) {
            var masterRestorer = new MasterRestorer(_liveConnectionString, _backupConnectionString);
            masterRestorer.Restore(masterIds);
        }
    }
}
