using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace DXP.Adam.Recovery.XPublish {
    internal abstract class CommandBuilder {
        public static List<SqlParameter> CreateParameters(string filterColumn, IEnumerable<Guid> filterIds) {
            var parameters = new List<SqlParameter>();
            for (int index = 0; index < filterIds.Count(); index++) {
                string parameterName = string.Format("{0}{1}", filterColumn, index + 1);
                var param = new SqlParameter(parameterName, filterIds.ElementAt(index));

                parameters.Add(param);
            }

            return parameters;
        }

        public static SqlCommand CreateCommand(string sql, List<SqlParameter> parameters) {
            var command = new SqlCommand {
                CommandText = sql,
                CommandType = CommandType.Text,
            };

            command.Parameters.AddRange(parameters.ToArray());
            return command;
        }

        public static SqlCommand PrepareCommand(string tableName, string filterColumn,
        IEnumerable<Guid> filterIds, Func<string, string, int, string> sqlBuilder) {
            string sql = sqlBuilder(tableName, filterColumn, filterIds.Count());
            var parameters = CreateParameters(filterColumn, filterIds);

            var command = CreateCommand(sql, parameters);

            return command;
        }
    }
}
