using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DXP.Adam.Recovery.XPublish {
    internal class SqlBuilder {
        private static readonly IDictionary<string, string> UpdateMapper = new Dictionary<string, string>();
        private static readonly ColumnMapper ColumnMapper = ColumnMapper.Instance;

        private static readonly IDictionary<string, IList<string>> TableMapper = new Dictionary<string, IList<string>>() {
              { TableName.XPublishTemplate, new[] { TableName.XPublishTemplate } },
              { TableName.Field, new[] {TableName.Field, TableName.XPublishTemplate } },
              { TableName.Ad,  new[] { TableName.Ad, TableName.XPublishTemplate } },
              { TableName.Value, new[] { TableName.Value, TableName.Field, TableName.XPublishTemplate } },
              { TableName.CommentEntry, new[] { TableName.CommentEntry, TableName.Ad, TableName.XPublishTemplate } },
              { TableName.OrderUserGroups, new[] { TableName.OrderUserGroups,TableName.Ad, TableName.XPublishTemplate } },
              { TableName.Master, new[] { TableName.Master }},
              { TableName.TagGroup, new[] { TableName.TagGroup, TableName.Master } },
              { TableName.Tag, new[] { TableName.Tag, TableName.TagGroup, TableName.Master } },
              { TableName.TextColor, new[] { TableName.TextColor, TableName.Master } },
              { TableName.CharacterStyles, new[] { TableName.CharacterStyles, TableName.Master } }
        };

        private static string BuildParameterString(string filterColumn, int parammeterCount) {
            var parameterBuilder = new StringBuilder();
            for (int index = 1; index <= parammeterCount; index++) {
                string parameter = string.Format("@{0}{1}", filterColumn, index);
                parameterBuilder.Append(parameter);

                if (index < parammeterCount) {
                    parameterBuilder.Append(",");
                }
            }

            return parameterBuilder.ToString();
        }
        
        private static string BuildClause(IList<string> columns, string separator) {
            var clauseBuilder = new StringBuilder();

            for (int index = 0; index < columns.Count; index++) {
                string column = columns[index];
                string formalColumn = string.Format("[{0}]", column);
                string parameter = string.Format("@{0}", column);

                clauseBuilder.AppendFormat("{0} = {1}", formalColumn, parameter);
                if (index < columns.Count - 1) {
                    clauseBuilder.Append(separator);
                }
            }

            return clauseBuilder.ToString();
        }

        private static string BuildSetClause(string tableName) {
            var columns = ColumnMapper.GetColumnsWithoutPrimaryKey(tableName);
            return BuildClause(columns, ", ");
        }

        private static string BuildWhereClause(string tableName) {
            var columns = ColumnMapper.GetPrimaryKeyColumns(tableName);
            return BuildClause(columns, " AND ");
        }

        public static string GetTemplatesFromMastersSql(string filterColumn, int parameterCount) {
            string joinCondition = JoinMapper.Instance.GetJoinCondition(TableName.XPublishTemplate, TableName.Master);
            string fromClause = string.Format("[{0}] INNER JOIN [{1}] ON ({2})",
                                              TableName.XPublishTemplate,
                                              TableName.Master,
                                              joinCondition);

            string sqlTemplate = "SELECT [{0}].* FROM {1} WHERE [{2}].[{3}] IN ({4})";
            string parameterString = BuildParameterString(filterColumn, parameterCount);

            string sql = string.Format(sqlTemplate,
                TableName.XPublishTemplate,
                fromClause,
                TableName.Master,
                filterColumn,
                parameterString);

            return sql;
        }

        public static string BuildUpdateSql(string tableName) {
            if (!UpdateMapper.ContainsKey(tableName)) {
                string setClause = BuildSetClause(tableName);
                string whereClause = BuildWhereClause(tableName);
                string sql = string.Format("UPDATE [{0}] SET {1} WHERE {2}", tableName, setClause, whereClause);
                UpdateMapper.Add(tableName, sql);

                return sql;
            }

            return UpdateMapper[tableName];
        }

        private static string BuildFromClause(string tableName) {
            var clauseBuilder = new StringBuilder();
            clauseBuilder.AppendFormat("[{0}] ", tableName);

            IList<string> tables = TableMapper[tableName];
            if (tables.Count() > 1) {
                for (int index = 0; index < (tables.Count() - 1); index++) {
                    string joinCondition = JoinMapper.Instance.GetJoinCondition(tables[index], tables[index + 1]);
                    clauseBuilder.AppendFormat("INNER JOIN [{0}] ON ({1}) ", tables[index + 1], joinCondition);
                }
            }

            return clauseBuilder.ToString();
        }

        private static string BuildSql(string sqlTemplate, string tableName, string filterColumn, int parameterCount) {
            string fromClause = BuildFromClause(tableName);
            string parameterString = BuildParameterString(filterColumn, parameterCount);

            string sql = string.Format(sqlTemplate, 
                tableName,
                fromClause,
                filterColumn, 
                parameterString);

            return sql;
        }

        public static string BuildSelectSql(string tableName, string filterColumn, int parameterCount) {
            string sqlTemplate = "SELECT [{0}].* FROM {1} WHERE [{2}] IN ({3})";
            return BuildSql(sqlTemplate, tableName, filterColumn, parameterCount);
        }

        public static string BuildDeleteSql(string tableName, string filterColumn, int parameterCount) {
            string sqlTemplate = "DELETE [{0}] FROM {1} WHERE [{2}] IN ({3})";
            return BuildSql(sqlTemplate, tableName, filterColumn, parameterCount);
        }
    }
}
