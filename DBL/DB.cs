using MySql.Data.MySqlClient;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using Microsoft.Data.SqlClient;
using static System.Runtime.InteropServices.JavaScript.JSType;
using System;


namespace DBL
{
    public abstract class DB
    {
        private enum DbTypes
        {
            MSAccess,
            MySql
        }

        private const DbTypes dbType = DbTypes.MySql;

        private const string MySqlConnSTR = @"server=localhost;
                                    user id=root;
                                    password=1234;
                                    persistsecurityinfo=True;
                                    database=mystore";

        private const string AccessConnSTR = $@"Provider=Microsoft.ACE.OLEDB.12.0;
                                    Data Source=C:\Users\Gadi\OneDrive\שולחן העבודה\Blazor\00_Code_files_2023\t13_project\00000_____________________DB_files\mystore.accdb;
                                    Persist Security Info=False;";

        protected static DbConnection conn;
        protected DbCommand cmd;
        protected DbDataReader reader;

        protected DB()
        {
            if (conn == null)
            {
                switch (dbType)
                {
                    case DbTypes.MSAccess:
                        conn = new OleDbConnection(AccessConnSTR);
                        cmd = new OleDbCommand();
                        break;
                    case DbTypes.MySql:
                        conn = new MySqlConnection(MySqlConnSTR);
                        cmd = new MySqlCommand();
                        break;
                    default:
                        throw new ArgumentException("Unsupported Db Type: ", dbType.ToString());
                }
            }
            switch (dbType)
            {
                case DbTypes.MSAccess:
                    cmd = new OleDbCommand();
                    break;
                case DbTypes.MySql:
                    cmd = new MySqlCommand();
                    break;
                default:
                    throw new ArgumentException("Unsupported Db Type: ", dbType.ToString());
            }
            cmd.Connection = conn;
            reader = null;
        }
    }
}