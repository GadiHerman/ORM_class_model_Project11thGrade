using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Models;

namespace DBL
{
    public class CustomerDB : BaseDB<Customer>
    {
        protected override string GetTableName()
        {
            return "Customers";
        }
        protected override Customer CreateModel(object[] row)
        {
            Customer c = new Customer();
            c.Id = int.Parse(row[0].ToString());
            c.Name = row[1].ToString();
            c.Email = row[2].ToString();
            c.IsAdmin = bool.Parse(row[4].ToString());
            return c;
        }
        protected override async Task<Customer> CreateModelAsync(object[] row)
        {
            Customer c = new Customer();
            c.Id = int.Parse(row[0].ToString());
            c.Name = row[1].ToString();
            c.Email = row[2].ToString();
            c.IsAdmin = bool.Parse(row[4].ToString());
            return c;
        }
        protected override List<Customer> CreateListModel(List<object[]> rows)
        {
            List<Customer> custList = new List<Customer>();
            foreach (object[] item in rows)
            {
                Customer c = new Customer();
                c = (Customer)CreateModel(item);
                custList.Add(c);
            }
            return custList;
        }
        protected override async Task<List<Customer>> CreateListModelAsync(List<object[]> rows)
        {
            List<Customer> custList = new List<Customer>();
            foreach (object[] item in rows)
            {
                Customer c = new Customer();
                c = (Customer)CreateModel(item);
                custList.Add(c);
            }
            return custList;
        }

        protected override async Task<Customer> GetRowByPKAsync(object pk)
        {
            string sql = @"SELECT customers.* FROM customers WHERE (CustomerID = @id)";
            cmd.Parameters.AddWithValue("@id", int.Parse(pk.ToString()));
            List<Customer> list = (List<Customer>)await SelectAllAsync(sql);
            if (list.Count == 1)
                return list[0];
            else
                return null;
        }

        protected override Customer GetRowByPK(object pk)
        {
            string sql = @"SELECT customers.* FROM customers WHERE (CustomerID = @id)";
            cmd.Parameters.AddWithValue("@id", int.Parse(pk.ToString()));
            List<Customer> list = (List<Customer>)SelectAll(sql);
            if (list.Count == 1)
                return list[0];
            else
                return null;
        }

        public async Task<List<Customer>> GetAllAsync()
        {
            return ((List<Customer>)await SelectAllAsync());
        }

        public List<Customer> GetAll()
        {
            return ((List<Customer>)SelectAll());
        }

        public async Task<bool> InsertAsync(Customer customer,string password)
        {
            Dictionary<string, string> fillValues = new Dictionary<string, string>
            {
                { "Name", customer.Name },
                { "Email", customer.Email },
                { "CustomerPassword", password }
            };
            return await base.InsertAsync(fillValues) == 1;
        }

        public bool Insert(Customer customer, string password)
        {
            Dictionary<string, string> fillValues = new Dictionary<string, string>
            {
                { "Name", customer.Name },
                { "Email", customer.Email },
                { "CustomerPassword", password }
            };
            return base.Insert(fillValues) == 1;
        }

        public async Task<Customer> InsertGetObjAsync(Customer customer, string password)
        {
            Dictionary<string, string> fillValues = new Dictionary<string, string>()
            {
                { "Name", customer.Name },
                { "Email", customer.Email },
                { "CustomerPassword", password }
            };
            return (Customer)base.InsertGetObjAsync(fillValues);
        }

        public Customer InsertGetObj(Customer customer, string password)
        {
            Dictionary<string, string> fillValues = new Dictionary<string, string>()
            {
                { "Name", customer.Name },
                { "Email", customer.Email },
                { "CustomerPassword", password }
            };
            return (Customer)base.InsertGetObj(fillValues);
        }

        public async Task<int> UpdateAsync(Customer customer)
        {
            Dictionary<string, string> fillValues = new Dictionary<string, string>();
            Dictionary<string, string> filterValues = new Dictionary<string, string>();
            fillValues.Add("Name", customer.Name);
            fillValues.Add("Email", customer.Email);
            filterValues.Add("CustomerID", customer.Id.ToString());
            return await base.UpdateAsync(fillValues, filterValues);
        }

        public int Update(Customer customer)
        {
            Dictionary<string, string> fillValues = new Dictionary<string, string>();
            Dictionary<string, string> filterValues = new Dictionary<string, string>();
            fillValues.Add("Name", customer.Name);
            fillValues.Add("Email", customer.Email);
            filterValues.Add("CustomerID", customer.Id.ToString());
            return base.Update(fillValues, filterValues);
        }

        public async Task<int> DeleteAsync(Customer customer)
        {
            Dictionary<string, string> filterValues = new Dictionary<string, string>
            {
                { "CustomerID", customer.Id.ToString() }
            };
            return await base.DeleteAsync(filterValues);
        }
        public int Delete(Customer customer)
        {
            Dictionary<string, string> filterValues = new Dictionary<string, string>
            {
                { "CustomerID", customer.Id.ToString() }
            };
            return base.Delete(filterValues);
        }

        public async Task<int> updatePasswordAsync(Customer customer,string password)
        {
            Dictionary<string, string> fillValues = new Dictionary<string, string>();
            Dictionary<string, string> filterValues = new Dictionary<string, string>();
            fillValues.Add("Name", customer.Name);
            fillValues.Add("Email", customer.Email);
            fillValues.Add("CustomerPassword", password);
            filterValues.Add("CustomerID", customer.Id.ToString());
            return await base.UpdateAsync(fillValues, filterValues);
        }

        public int updatePassword(Customer customer, string password)
        {
            Dictionary<string, string> fillValues = new Dictionary<string, string>();
            Dictionary<string, string> filterValues = new Dictionary<string, string>();
            fillValues.Add("Name", customer.Name);
            fillValues.Add("Email", customer.Email);
            fillValues.Add("CustomerPassword", password);
            filterValues.Add("CustomerID", customer.Id.ToString());
            return base.Update(fillValues, filterValues);
        }









        // specific queries
        public async Task<string> GetPasswordAsync(int id)
        {
            string sql = @"SELECT customers.CustomerPassword FROM customers WHERE (CustomerID = @id)";
            cmd.Parameters.AddWithValue("@id", id);
            string oldPassword = (string)await exeScalarAsync(sql);
            return oldPassword;
        }

        public string GetPassword(int id)
        {
            string sql = @"SELECT customers.CustomerPassword FROM customers WHERE (CustomerID = @id)";
            cmd.Parameters.AddWithValue("@id", id);
            string oldPassword = (string)exeScalar(sql);
            return oldPassword;
        }

        public async Task<Customer> SelectByPkAsync(int id)
        {
            string sql = @"SELECT customers.* FROM customers WHERE (CustomerID = @id)";
            cmd.Parameters.AddWithValue("@id", id);
            List<Customer> list = (List<Customer>)await SelectAllAsync(sql);
            if (list.Count == 1)
                return list[0];
            else
                return null;
        }

        public Customer SelectByPk(int id)
        {
            string sql = @"SELECT customers.* FROM customers WHERE (CustomerID = @id)";
            cmd.Parameters.AddWithValue("@id", id);
            List<Customer> list = (List<Customer>)SelectAll(sql);
            if (list.Count == 1)
                return list[0];
            else
                return null;
        }

        public async Task<List<Customer>> GetNonAdminsAsync()
        {
            Dictionary<string, string> p = new Dictionary<string, string>();
            p.Add("IsAdmin", "0");
            return ((List<Customer>)await SelectAllAsync(p));
        }

        public List<Customer> GetNonAdmins()
        {
            Dictionary<string, string> p = new Dictionary<string, string>();
            p.Add("IsAdmin", "0");
            return ((List<Customer>)SelectAll(p));
        }

        public async Task<List<(string, string)>> GetName_Email4NonAdminsAsync()
        {
            List<(string, string)> returnList = new List<(string, string)>();
            string sql = "select Name, Email from Customers";
            Dictionary<string, string> p = new Dictionary<string, string>();
            p.Add("IsAdmin", "0");
            List<object[]> list = (List<object[]>)await base.StingListSelectAllAsync(sql, p);
            foreach (object[] item in list)
            {
                string name = item[0].ToString();
                string email = item[1].ToString();
                returnList.Add((name, email));
            }
            return returnList;
        }

        public List<(string, string)> GetName_Email4NonAdmins()
        {
            List<(string, string)> returnList = new List<(string, string)>();
            string sql = "select Name, Email from Customers";
            Dictionary<string, string> p = new Dictionary<string, string>();
            p.Add("IsAdmin", "0");
            List<object[]> list = (List<object[]>)base.StingListSelectAll(sql, p);
            foreach (object[] item in list)
            {
                string name = item[0].ToString();
                string email = item[1].ToString();
                returnList.Add((name, email));
            }
            return returnList;
        }

        public async Task<Customer> GetCustomerByOrderIDAsync(int orderID)
        {
            string sql = @$"Select mystore.customers.*
                           From mystore.customers Inner Join mystore.orders 
                           On mystore.orders.CustomerID = mystore.customers.CustomerID";
            Dictionary<string, string> p = new Dictionary<string, string>();
            p.Add("mystore.orders.OrderID", orderID.ToString());
            List<Customer> list = (List<Customer>)await SelectAllAsync(sql, p);
            if (list.Count == 1)
                return list[0];
            else
                return null;
        }

        public Customer GetCustomerByOrderID(int orderID)
        {
            string sql = @$"Select mystore.customers.*
                           From mystore.customers Inner Join mystore.orders 
                           On mystore.orders.CustomerID = mystore.customers.CustomerID";
            Dictionary<string, string> p = new Dictionary<string, string>();
            p.Add("mystore.orders.OrderID", orderID.ToString());
            List<Customer> list = (List<Customer>)SelectAll(sql, p);
            if (list.Count == 1)
                return list[0];
            else
                return null;
        }

    }
}
