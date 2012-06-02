using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Data;

namespace DXP.Adam.Recovery.XPublish {
    internal class JoinMapper {
        private static readonly JoinMapper _instance = new JoinMapper();
        private readonly IDictionary<string, string> _mapper = new Dictionary<string, string>();

        private JoinMapper() {
        }

        public static JoinMapper Instance {
            get { return _instance; }
        }

        private static DbCommand PrepareCommand(SqlConnection connection, string sql, string foreignTable, string primaryTable) {
            var command = new SqlCommand {
                CommandText = sql,
                CommandType = CommandType.Text,
                Connection = connection
            };

            command.Parameters.AddWithValue("ForeignTable", foreignTable);
            command.Parameters.AddWithValue("PrimaryTable", primaryTable);

            return command;
        }

        private static string DoGetJoinCondition(string foreignTable, string primaryTable) {
            string sql = SqlClause.SelectRelationShip;
            string connectionString = ConfigurationManager.ConnectionStrings["Backup"].ConnectionString;

            string joinCondition = string.Empty;

            using (var connection = new SqlConnection(connectionString)) {
                connection.Open();

                var command = PrepareCommand(connection, sql, foreignTable, primaryTable);
                using (var reader = command.ExecuteReader()) {
                    if (reader != null) {
                        while (reader.Read()) {
                            string foreignColumn = reader.GetString(1);
                            string primaryColumn = reader.GetString(3);

                            joinCondition = string.Format("[{0}].[{1}] = [{2}].[{3}]",
                                foreignTable,
                                foreignColumn,
                                primaryTable,
                                primaryColumn);
                        }
                    }
                }
            }

            return joinCondition;
        }

        public string GetJoinCondition(string foreignTable, string primaryTable) {
            string key = string.Format("{0}-{1}", foreignTable, primaryTable);

            if (!_mapper.ContainsKey(key)) {
                string joinCondition = DoGetJoinCondition(foreignTable, primaryTable);
                _mapper.Add(key, joinCondition);
            }

            return _mapper[key];
        }
    }
}
