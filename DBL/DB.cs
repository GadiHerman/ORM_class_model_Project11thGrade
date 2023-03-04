using MySql.Data.MySqlClient;
using System.Collections.Generic;

namespace DBL
{
    public abstract class DB
    {
        private const string connSTR = @"server=localhost;
                                    user id=root;
                                    password=1234;
                                    persistsecurityinfo=True;
                                    database=mystore";
        protected static MySqlConnection conn;
        protected MySqlCommand cmd;
        protected MySqlDataReader reader;

        protected DB()
        {
            if (conn == null)
            {
                conn = new MySqlConnection(connSTR);
            }
            cmd = new MySqlCommand();
            cmd.Connection = conn;
        }

        protected async Task<object> getDataFromDBAsync(string sqlCommand)
        {
            object list = new List<object[]>();
            cmd.CommandText = sqlCommand;
            if (DB.conn.State != System.Data.ConnectionState.Open)
                DB.conn.Open();
            if (cmd.Connection.State != System.Data.ConnectionState.Open)
                cmd.Connection = DB.conn;
            try
            {
                this.reader = (MySqlDataReader)await cmd.ExecuteReaderAsync();
                var readOnlyData = await reader.GetColumnSchemaAsync();
                int size = readOnlyData.Count;
                object[] row;
                while (this.reader.Read())
                {
                    row = new object[size];
                    this.reader.GetValues(row);
                    ((List<object[]>)list).Add(row);
                }
            }
            catch (Exception e)
            {
                System.Diagnostics.Debug.WriteLine(e.Message + "\nsql:" + cmd.CommandText);
                //((List<string[]>)list).Clear();
            }
            finally
            {
                cmd.Parameters.Clear();
                if (reader != null) reader.Close();
                if (DB.conn.State == System.Data.ConnectionState.Open)
                    DB.conn.Close();
            }
            return list;
        }
    }
}