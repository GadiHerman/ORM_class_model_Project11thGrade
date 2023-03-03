using Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBL
{
    public class OrderDB : BaseDB<Order>
    {
        protected override string GetTableName()
        {
            return "Orders";
        }
        protected override Order CreateModel(object[] row)
        {
            Order o = new Order();
            o.Id = int.Parse(row[0].ToString());
            o.OrderDateTime = DateTime.Parse(row[1].ToString());
            int custID = int.Parse(row[2].ToString());
            o.customer = new CustomerDB().SelectByPk(custID);
            return o;
        }
        protected override async Task<Order> CreateModelAsync(object[] row)
        {
            Order o = new Order();
            o.Id = int.Parse(row[0].ToString());
            o.OrderDateTime = DateTime.Parse(row[1].ToString());
            int custID = int.Parse(row[2].ToString());
            Customer customer = new Customer();
            CustomerDB customerDB = new CustomerDB();
            customer = await customerDB.SelectByPkAsync(custID);
            o.customer = customer;
            return o;
        }
        protected override List<Order> CreateListModel(List<object[]> rows)
        {
            List<Order> orderList = new List<Order>();
            foreach (object[] item in rows)
            {
                Order o = new Order();
                o = (Order)CreateModel(item);
                orderList.Add(o);
            }
            return orderList;
        }
        protected override async Task<List<Order>> CreateListModelAsync(List<object[]> rows)
        {
            List<Order> orderList = new List<Order>();
            foreach (object[] item in rows)
            {
                Order o = new Order();
                o = (Order)await CreateModelAsync(item);
                orderList.Add(o);
            }
            return orderList;
        }
        protected override async Task<Order> GetRowByPKAsync(object pk)
        {
            string sql = @"SELECT orders.* FROM orders WHERE (OrderID = @id)";
            cmd.Parameters.AddWithValue("@id", int.Parse(pk.ToString()));
            List<Order> list = (List<Order>)await SelectAllAsync(sql);
            if (list.Count == 1)
                return list[0];
            else
                return null;
        }

        protected override Order GetRowByPK(object pk)
        {
            string sql = @"SELECT orders.* FROM orders WHERE (OrderID = @id)";
            cmd.Parameters.AddWithValue("@id", int.Parse(pk.ToString()));
            List<Order> list = (List<Order>)SelectAll(sql);
            if (list.Count == 1)
                return list[0];
            else
                return null;
        }

        public async Task<List<Order>> GetAllAsync()
        {
            return ((List<Order>)await SelectAllAsync());
        }
        
        
        
        public async Task<List<Order>> GetAllOrdersByCustomerIDAsync(int customerID)
        {
            Dictionary<string, string> p = new Dictionary<string, string>();
            p.Add("CustomerID", customerID.ToString());
            return ((List<Order>)await SelectAllAsync(p));
        }
        public async Task<Order> GetOrderByPkAsync(int OrderID)
        {
            Dictionary<string, string> p = new Dictionary<string, string>();
            p.Add("OrderID", OrderID.ToString());
            List<Order> list = (List<Order>)await SelectAllAsync(p);
            if (list.Count == 1)
                return list[0];
            else
                return null;
        }

        public async Task<bool> InsertAsync(Order order)
        {
            Dictionary<string, string> fillValues = new Dictionary<string, string>();
            DateTime d = DateTime.Now;
            string dts = $"'{d.Year}-{d.Month}-{d.Day} {d.Hour}:{d.Minute}:{d.Second}'";
            fillValues.Add("OrderDateTime", dts);
            fillValues.Add("CustomerID", order.customer.Id.ToString());
            return await base.InsertAsync(fillValues) == 1;
        }

        public Order InsertGetObjAsync(Order order)
        {
            Dictionary<string, string> fillValues = new Dictionary<string, string>();
            DateTime d = DateTime.Now;
            string dts = $"'{d.Year}-{d.Month}-{d.Day} {d.Hour}:{d.Minute}:{d.Second}'";
            fillValues.Add("OrderDateTime", dts);
            fillValues.Add("CustomerID", order.customer.Id.ToString());
            return (Order)base.InsertGetObjAsync(fillValues);
        }

        public async Task<int> UpdateAsync(Order order)
        {
            Dictionary<string, string> fillValues = new Dictionary<string, string>();
            Dictionary<string, string> filterValues = new Dictionary<string, string>();
            DateTime d = DateTime.Now;
            string dts = $"'{d.Year}-{d.Month}-{d.Day} {d.Hour}:{d.Minute}:{d.Second}'";
            fillValues.Add("OrderDateTime", dts);
            filterValues.Add("OrderID", order.Id.ToString());
            return await base.UpdateAsync(fillValues, filterValues);
        }

        public async Task<int> DeleteAsync(Order order)
        {
            Dictionary<string, string> filterValues = new Dictionary<string, string>();
            filterValues.Add("OrderID", order.Id.ToString());
            return await base.DeleteAsync(filterValues);
        }
    }
}
