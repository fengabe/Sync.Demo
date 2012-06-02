using System.Text;

namespace DXP.Adam.Recovery.XPublish {
    internal class SqlClause {
        public static string SelectColumn {
            get {
                return BuildSqlTemplate(
                    "SELECT [column_name] FROM [information_schema].[columns] ",
                    "WHERE [table_name] = @TableName ",
                    "ORDER BY [ordinal_position]"
                    );
            }
        }

        public static string SelectPrimaryColumns {
            get {
                return BuildSqlTemplate(
                    "SELECT [syscolumns].[name] FROM [syscolumns] ",
                    "INNER JOIN [sysobjects] ",
                    "ON ([syscolumns].[id] = [sysobjects].[id]) ",
                    "INNER JOIN [sysindexkeys] ",
                    "ON ([syscolumns].[colid] = [sysindexkeys].[colid] AND [sysobjects].[id] = [sysindexkeys].[id]) ",
                    "WHERE [sysobjects].[name] = @TableName ",
                    "AND [sysindexkeys].indid = 1"
                    );
            }
        }

        public static string SelectRelationShip {
            get {
                return BuildSqlTemplate(
                    "SELECT ",
                        "FK_Table = FK.TABLE_NAME, ",
                        "FK_Column = CU.COLUMN_NAME, ",
                        "PK_Table = PK.TABLE_NAME, ",
                        "PK_Column = PT.COLUMN_NAME, ",
                        "Constraint_Name = C.CONSTRAINT_NAME ",
                    "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C ",
                    "INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME ",
                    "INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME ",
                    "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME ",
                    "INNER JOIN ( ",
                        "SELECT i1.TABLE_NAME, i2.COLUMN_NAME ",
                        "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1 ",
                        "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2 ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME ",
                        "WHERE i1.CONSTRAINT_TYPE = 'PRIMARY KEY' ",
                    ") PT ON PT.TABLE_NAME = PK.TABLE_NAME ",
                    "WHERE FK.TABLE_NAME = @ForeignTable AND PK.TABLE_NAME = @PrimaryTable"
                    );
            }
        }

        private static string BuildSqlTemplate(params string[] parts) {
            var builder = new StringBuilder();

            foreach (string part in parts) {
                builder.Append(part);
            }

            return builder.ToString();
        }
    }
}
