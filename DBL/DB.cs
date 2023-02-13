using MySql.Data.MySqlClient;

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

        protected DB()
        {
            conn = new MySqlConnection(connSTR);
            cmd = new MySqlCommand();
            cmd.Connection = conn;
        }
    }

}