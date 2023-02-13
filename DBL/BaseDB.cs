using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBL
{
    public abstract class BaseDB : DB
    {
        protected MySqlDataReader reader;
        protected abstract string GetTableName();
        protected abstract object GetRowByPK(object pk);
        protected abstract Task<object> GetRowByPKAsync(object pk);
        protected abstract object CreateModel(object[] row);
        protected abstract Task<object> CreateModelAsync(object[] row);
        protected abstract object CreateListModel(List<object[]> rows);
        protected abstract Task<object> CreateListModelAsync(List<object[]> rows);

        public object SelectAll()
        {
            List<object[]> list = (List<object[]>)StingListSelectAll("", new Dictionary<string, string>());
            return CreateListModel(list);
        }
        public object SelectAll(Dictionary<string, string> parameters)
        {
            List<object[]> list = (List<object[]>)StingListSelectAll("", parameters);
            return CreateListModel(list);
        }
        public object SelectAll(string query)
        {
            List<object[]> list = (List<object[]>)StingListSelectAll(query, new Dictionary<string, string>());
            return CreateListModel(list);
        }
        public object SelectAll(string query, Dictionary<string, string> parameters)
        {
            List<object[]> list = (List<object[]>)StingListSelectAll(query, parameters);
            return CreateListModel(list);
        }
        protected object StingListSelectAll(string query, Dictionary<string, string> parameters)
        {
            object list = new List<object[]>();
            string where = "WHERE ";
            if (parameters != null && parameters.Count > 0)
            {
                int i = 0;
                foreach (KeyValuePair<string, string> param in parameters)
                {
                    where += $"{param.Key} = {param.Value}";
                    i++;
                    if (i < parameters.Count)
                        where += " AND ";
                }
            }
            else
                where = "";
            string sqlCommand = $"{query} {where}";
            if (String.IsNullOrEmpty(query))
                sqlCommand = $"SELECT * FROM {GetTableName()} {where}";
            base.cmd.CommandText = sqlCommand;
            if (DB.conn.State != System.Data.ConnectionState.Open)
                DB.conn.Open();
            if (base.cmd.Connection.State != System.Data.ConnectionState.Open)
                base.cmd.Connection = DB.conn;

            try
            {
                this.reader = base.cmd.ExecuteReader();
                int size = reader.GetColumnSchema().ToArray().Length;
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
                ((List<string[]>)list).Clear();
            }
            finally
            {
                base.cmd.Parameters.Clear();
                if (reader != null) reader.Close();
                if (DB.conn.State == System.Data.ConnectionState.Open)
                    DB.conn.Close();
            }
            return list;
        }

        // exeNONquery
        protected int exeNONquery(string query)
        {
            if (String.IsNullOrEmpty(query))
                return 0;
            base.cmd.CommandText = query;
            if (DB.conn.State != System.Data.ConnectionState.Open)
                DB.conn.Open();
            if (base.cmd.Connection.State != System.Data.ConnectionState.Open)
                base.cmd.Connection = DB.conn;
            int rowsEffected = 0;
            try
            {
                rowsEffected = base.cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                System.Diagnostics.Debug.WriteLine(e.Message + "\nsql:" + cmd.CommandText);
            }
            finally
            {
                base.cmd.Parameters.Clear();
                if (DB.conn.State == System.Data.ConnectionState.Open)
                    DB.conn.Close();
            }
            return rowsEffected;
        }

        // exeScalar
        public object exeScalar(string query)
        {
            if (String.IsNullOrEmpty(query))
                return null;
            base.cmd.CommandText = query;
            if (DB.conn.State != System.Data.ConnectionState.Open)
                DB.conn.Open();
            if (base.cmd.Connection.State != System.Data.ConnectionState.Open)
                base.cmd.Connection = DB.conn;
            object obj = null;
            try
            {
                obj = base.cmd.ExecuteScalar();
            }
            catch (Exception e)
            {
                System.Diagnostics.Debug.WriteLine(e.Message + "\nsql:" + cmd.CommandText);
            }
            finally
            {
                base.cmd.Parameters.Clear();
                if (DB.conn.State == System.Data.ConnectionState.Open)
                    DB.conn.Close();
            }
            return obj;
        }

        // Dictionary<string, string> FildValue - ערכים של שדות
        // return -  מספר שדות שעודכנו
        protected int Insert(Dictionary<string, string> FildValue)
        {
            string InKey = $"(";
            string InValue = "VALUES (";
            if (FildValue != null && FildValue.Count > 0)
            {
                int i = 0;
                foreach (KeyValuePair<string, string> param in FildValue)
                {
                    InKey += $"{param.Key}";
                    InValue += $"'{param.Value}'";
                    i++;
                    if (i < FildValue.Count)
                    {
                        InKey += ",";
                        InValue += ",";
                    }
                }
                InKey += ")";
                InValue += ")";
            }
            else
                return 0;
            string sqlCommand = $"INSERT INTO {GetTableName()}  {InKey} {InValue}";
            return exeNONquery(sqlCommand);
        }

        protected object InsertGetObj(Dictionary<string, string> FildValue)
        {
            string InKey = $"(";
            string InValue = "VALUES (";
            if (FildValue != null && FildValue.Count > 0)
            {
                int i = 0;
                foreach (KeyValuePair<string, string> param in FildValue)
                {
                    InKey += $"{param.Key}";
                    InValue += $"{param.Value}";
                    i++;
                    if (i < FildValue.Count)
                    {
                        InKey += ",";
                        InValue += ",";
                    }
                }
                InKey += ")";
                InValue += ")";
            }
            else
                return null;
            string sqlCommand = $"INSERT INTO {GetTableName()}  {InKey} {InValue};" +
                                $"SELECT LAST_INSERT_ID();";
            object res = exeScalar(sqlCommand);
            if (res != null)
            {
                ulong qkwl = (ulong)res;
                int Id = (int)qkwl;
                return GetRowByPK(Id);
            }
            else
                return null;
        }


        // Dictionary<string, string> FildValue - ערכים של שדות
        // Dictionary<string, string> parameters - תנאים לעדכון
        // return -  מספר שדות שעודכנו

        protected int Update(Dictionary<string, string> FildValue, Dictionary<string, string> parameters)
        {
            string InKeyValue = "";
            string where = "WHERE ";

            if (parameters != null && parameters.Count > 0)
            {
                int i = 0;
                foreach (KeyValuePair<string, string> param in parameters)
                {
                    where += $"{param.Key} = {param.Value}";
                    i++;
                    if (i < parameters.Count)
                        where += " AND ";
                }
            }
            else
                where = "";

            if (FildValue != null && FildValue.Count > 0)
            {
                int i = 0;
                foreach (KeyValuePair<string, string> param in FildValue)
                {
                    InKeyValue += $"{param.Key} = '{param.Value}'";
                    i++;
                    if (i < FildValue.Count)
                    {
                        InKeyValue += ",";
                    }
                }
            }
            else
                return 0;
            string sqlCommand = $"UPDATE {GetTableName()} SET {InKeyValue}  {where}";
            return exeNONquery(sqlCommand);
        }

        // Dictionary<string, string> parameters - תנאים לעדכון
        // return -  מספר שדות שעודכנו

        protected int Delete(Dictionary<string, string> parameters)
        {
            string where = "WHERE ";

            if (parameters != null && parameters.Count > 0)
            {
                int i = 0;
                foreach (KeyValuePair<string, string> param in parameters)
                {
                    where += $"{param.Key} = {param.Value}";
                    i++;
                    if (i < parameters.Count)
                        where += " AND ";
                }
            }
            else
                where = "";

            string sqlCommand = $"DELETE FROM {GetTableName()} {where}";
            return exeNONquery(sqlCommand);
        }


        public async Task<object> SelectAllAsync()
        {
            List<object[]> list = (List<object[]>)await StingListSelectAllAsync("", new Dictionary<string, string>());
            return CreateListModel(list);
        }

        public async Task<object> SelectAllAsync(Dictionary<string, string> parameters)
        {
            List<object[]> list = (List<object[]>)await StingListSelectAllAsync("", parameters);
            return CreateListModel(list);
        }
        public async Task<object> SelectAllAsync(string query)
        {
            List<object[]> list = (List<object[]>)await StingListSelectAllAsync(query, new Dictionary<string, string>());
            return CreateListModel(list);
        }
        public async Task<object> SelectAllAsync(string query, Dictionary<string, string> parameters)
        {
            List<object[]> list = (List<object[]>)await StingListSelectAllAsync(query, parameters);
            return CreateListModel(list);
        }
        protected async Task<object> StingListSelectAllAsync(string query, Dictionary<string, string> parameters)
        {
            object list = new List<object[]>();
            string where = "WHERE ";
            if (parameters != null && parameters.Count > 0)
            {
                int i = 0;
                foreach (KeyValuePair<string, string> param in parameters)
                {
                    where += $"{param.Key} = {param.Value}";
                    i++;
                    if (i < parameters.Count)
                        where += " AND ";
                }
            }
            else
                where = "";
            string sqlCommand = $"{query} {where}";
            if (String.IsNullOrEmpty(query))
                sqlCommand = $"SELECT * FROM {GetTableName()} {where}";
            base.cmd.CommandText = sqlCommand;
            if (DB.conn.State != System.Data.ConnectionState.Open)
                DB.conn.Open();
            if (base.cmd.Connection.State != System.Data.ConnectionState.Open)
                base.cmd.Connection = DB.conn;

            try
            {
                this.reader = (MySqlDataReader)await base.cmd.ExecuteReaderAsync();
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
                ((List<string[]>)list).Clear();
            }
            finally
            {
                base.cmd.Parameters.Clear();
                if (reader != null) reader.Close();
                if (DB.conn.State == System.Data.ConnectionState.Open)
                    DB.conn.Close();
            }
            return list;
        }

        // exeNONquery
        protected async Task<int> exeNONqueryAsync(string query)
        {
            if (String.IsNullOrEmpty(query))
                return 0;
            base.cmd.CommandText = query;
            if (DB.conn.State != System.Data.ConnectionState.Open)
                DB.conn.Open();
            if (base.cmd.Connection.State != System.Data.ConnectionState.Open)
                base.cmd.Connection = DB.conn;
            int rowsEffected = 0;
            try
            {
                rowsEffected = await base.cmd.ExecuteNonQueryAsync();
            }
            catch (Exception e)
            {
                System.Diagnostics.Debug.WriteLine(e.Message + "\nsql:" + cmd.CommandText);
            }
            finally
            {
                base.cmd.Parameters.Clear();
                if (DB.conn.State == System.Data.ConnectionState.Open)
                    DB.conn.Close();
            }
            return rowsEffected;
        }

        // exeScalar
        public async Task<object> exeScalarAsync(string query)
        {
            if (String.IsNullOrEmpty(query))
                return null;
            base.cmd.CommandText = query;
            if (DB.conn.State != System.Data.ConnectionState.Open)
                DB.conn.Open();
            if (base.cmd.Connection.State != System.Data.ConnectionState.Open)
                base.cmd.Connection = DB.conn;
            object obj = null;
            try
            {
                obj = await base.cmd.ExecuteScalarAsync();
            }
            catch (Exception e)
            {
                System.Diagnostics.Debug.WriteLine(e.Message + "\nsql:" + cmd.CommandText);
            }
            finally
            {
                base.cmd.Parameters.Clear();
                if (DB.conn.State == System.Data.ConnectionState.Open)
                    DB.conn.Close();
            }
            return obj;
        }

        // Dictionary<string, string> FildValue - ערכים של שדות
        // return -  מספר שדות שעודכנו
        protected async Task<int> InsertAsync(Dictionary<string, string> FildValue)
        {
            string InKey = $"(";
            string InValue = "VALUES (";
            if (FildValue != null && FildValue.Count > 0)
            {
                int i = 0;
                foreach (KeyValuePair<string, string> param in FildValue)
                {
                    InKey += $"{param.Key}";
                    InValue += $"'{param.Value}'";
                    i++;
                    if (i < FildValue.Count)
                    {
                        InKey += ",";
                        InValue += ",";
                    }
                }
                InKey += ")";
                InValue += ")";
            }
            else
                return 0;
            string sqlCommand = $"INSERT INTO {GetTableName()}  {InKey} {InValue}";
            return await exeNONqueryAsync(sqlCommand);
        }

        protected object InsertGetObjAsync(Dictionary<string, string> FildValue)
        {
            string InKey = $"(";
            string InValue = "VALUES (";
            if (FildValue != null && FildValue.Count > 0)
            {
                int i = 0;
                foreach (KeyValuePair<string, string> param in FildValue)
                {
                    InKey += $"{param.Key}";
                    InValue += $"{param.Value}";
                    i++;
                    if (i < FildValue.Count)
                    {
                        InKey += ",";
                        InValue += ",";
                    }
                }
                InKey += ")";
                InValue += ")";
            }
            else
                return null;
            string sqlCommand = $"INSERT INTO {GetTableName()}  {InKey} {InValue};" +
                                $"SELECT LAST_INSERT_ID();";
            object res = exeScalarAsync(sqlCommand);
            if (res != null)
            {
                ulong qkwl = (ulong)res;
                int Id = (int)qkwl;
                return GetRowByPK(Id);
            }
            else
                return null;
        }


        // Dictionary<string, string> FildValue - ערכים של שדות
        // Dictionary<string, string> parameters - תנאים לעדכון
        // return -  מספר שדות שעודכנו

        protected async Task<int> UpdateAsync(Dictionary<string, string> FildValue, Dictionary<string, string> parameters)
        {
            string InKeyValue = "";
            string where = "WHERE ";

            if (parameters != null && parameters.Count > 0)
            {
                int i = 0;
                foreach (KeyValuePair<string, string> param in parameters)
                {
                    where += $"{param.Key} = {param.Value}";
                    i++;
                    if (i < parameters.Count)
                        where += " AND ";
                }
            }
            else
                where = "";

            if (FildValue != null && FildValue.Count > 0)
            {
                int i = 0;
                foreach (KeyValuePair<string, string> param in FildValue)
                {
                    InKeyValue += $"{param.Key} = '{param.Value}'";
                    i++;
                    if (i < FildValue.Count)
                    {
                        InKeyValue += ",";
                    }
                }
            }
            else
                return 0;
            string sqlCommand = $"UPDATE {GetTableName()} SET {InKeyValue}  {where}";
            return await exeNONqueryAsync(sqlCommand);
        }

        // Dictionary<string, string> parameters - תנאים לעדכון
        // return -  מספר שדות שעודכנו

        protected async Task<int> DeleteAsync(Dictionary<string, string> parameters)
        {
            string where = "WHERE ";

            if (parameters != null && parameters.Count > 0)
            {
                int i = 0;
                foreach (KeyValuePair<string, string> param in parameters)
                {
                    where += $"{param.Key} = {param.Value}";
                    i++;
                    if (i < parameters.Count)
                        where += " AND ";
                }
            }
            else
                where = "";

            string sqlCommand = $"DELETE FROM {GetTableName()} {where}";
            return await exeNONqueryAsync(sqlCommand);
        }
    }
}
