using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Json;

namespace Scalien
{
    public class Test
    {
        // test entry point
        public static void Main(string[] args)
        {
            //Client.SetTrace(true);
            string[] controllers = { "192.168.1.234:7080" };
            Client client = new Client(controllers);
            Database db = client.GetDatabase("test");
            Table table = db.GetTable("test");
            table.Set("foo", "foo");
            System.Console.WriteLine(table.Get("foo"));
            if (table.Get("bar") == null)
                System.Console.WriteLine("null");
        }
    }
}
