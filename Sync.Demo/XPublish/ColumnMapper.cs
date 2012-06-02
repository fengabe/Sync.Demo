using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;

namespace DXP.Adam.Recovery.XPublish {
    internal class ColumnMapper {
        private static readonly ColumnMapper _instance = new ColumnMapper();
        private readonly IDictionary<string, IList<string>> _mapper = new Dictionary<string, IList<string>>();
        private readonly IDictionary<string, IList<string>> _primaryKeyMapper = new Dictionary<string, IList<string>>();

        private ColumnMapper()
            : this(
                ConfigurationManager.ConnectionStrings["Backup"].ConnectionString,
                TableName.XPublishTemplate,
                TableName.Field,
                TableName.Ad,
                TableName.Value,
                TableName.CommentEntry,
                TableName.OrderUserGroups,
                TableName.Master,
                TableName.TagGroup,
                TableName.Tag,
                TableName.TextColor,
                TableName.CharacterStyles
                ) { }

        private ColumnMapper(string connectionString, params string[] tableNames) {
            using (var connection = new SqlConnection(connectionString)) {
                connection.Open();

                foreach (string tableName in tableNames) {
                    var columnNames = GetAllColumnNames(connection, tableName);
                    _mapper.Add(tableName, columnNames);

                    var primaryColumnNames = GetPrimaryColumnNames(connection, tableName);
                    _primaryKeyMapper.Add(tableName, primaryColumnNames);

                }
            }
        }

        public static ColumnMapper Instance {
            get { return _instance; }
        }

        public IList<string> this[string tableName] {
            get {
                return _mapper[tableName];
            }
        }

        public IList<string> GetColumnsWithoutPrimaryKey(string tableName) {
            var columns = _mapper[tableName];
            var primaryKey = _primaryKeyMapper[tableName];

            return columns.Except(primaryKey).ToList();
        }

        public IList<string> GetPrimaryKeyColumns(string tableName) {
            var primaryKeyColumns = _primaryKeyMapper[tableName];
            return primaryKeyColumns;
        }

        private static DbCommand PrepareCommand(SqlConnection connection, string sql, string tableName) {
            var command = new SqlCommand {
                CommandText = sql,
                CommandType = CommandType.Text,
                Connection = connection
            };

            var parameter = new SqlParameter("TableName", tableName);
            command.Parameters.Add(parameter);

            return command;
        }

        private static IList<string> GetColumnNames(SqlConnection connection, string sql, string tableName) {
            var command = PrepareCommand(connection, sql, tableName);
            var columnNames = new List<string>();

            using (var reader = command.ExecuteReader()){
                while (reader.Read()) {
                    var columnName = reader.GetString(0);
                    columnNames.Add(columnName);
                }
            }

            return columnNames;
        }

        private static IList<string> GetAllColumnNames(SqlConnection connection, string tableName) {
            string sql = SqlClause.SelectColumn;
            return GetColumnNames(connection, sql, tableName);
        }

        private static IList<string> GetPrimaryColumnNames(SqlConnection connection, string tableName) {
            string sql = SqlClause.SelectPrimaryColumns;
            return GetColumnNames(connection, sql, tableName);
        }
    }
}
