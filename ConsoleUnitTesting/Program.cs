using Models;
using DBL;
namespace ConsoleUnitTesting
{
    internal class Program
    {
        static void Main(string[] args)
        {
            //Test 
            CustomerDB db = new CustomerDB();
            Customer customer= new Customer();
            customer.Name = "tal";
            customer.Email = "tal@gmail.com";
            customer = db.InsertGetObj(customer, "1234");
            Console.WriteLine($"id: {customer.Id} name: {customer.Name} email: {customer.Email}\n\n");

            //TEST CustomerDB Get by id
            CustomerDB cdb = new CustomerDB();
            Dictionary<string, string> param = new Dictionary<string, string>();
            param.Add("CustomerID", "1");
            List<Customer> list = cdb.SelectAll(param);
            if (list != null)
            {
                Console.WriteLine($" name: {list[0].Name} email: {list[0].Email}\n\n");
            }

            //TEST CustomerDB SelectAll
            list = cdb.SelectAll();
            for (int i = 0; i < list.Count; i++)
            {
                Console.WriteLine(@$" id={list[i].Id} name={list[i].Name} email={list[i].Email}");
            }
        }
    }
}