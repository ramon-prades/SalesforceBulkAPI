using SalesforceBulkAPI;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            QueryAgent agent = new QueryAgent("Account", "select Id, Name from Account limit 10");
            agent.SetProxy("http://172.22.2.128:8080", "es00500133", "contrasena_09");
            agent.Connect();

        }
    }
}
