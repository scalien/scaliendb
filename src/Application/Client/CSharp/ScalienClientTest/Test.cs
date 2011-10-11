using System;
using System.IO;
using System.Collections.Generic;
using System.Runtime.Serialization.Json;

namespace Scalien
{
    public class Test
    {
        // test entry point
        public static void Main(string[] args)
        {
            //Client.SetTrace(true);
            string[] controllers = { "127.0.0.1:7080" };
            Client client = new Client(controllers);
            Database db = client.GetDatabase("test");
            Table table = db.GetTable("test");
            //System.Random random = new System.Random();
            //var value = "";
            //while (value.Length < 10 * 1000)
            //    value += "" + random.Next();
            //using (client.Begin())
            //{
            //    for (var i = 0; i < 10*1000; i++)
            //    {
            //        var key = i.ToString("D9");
            //        table.Set(key, value);
            //    }
            //}

            StringRangeParams ps = new StringRangeParams().Prefix("00000006").StartKey("000000065").Backward();
            foreach (string key in table.GetKeyIterator(ps))
            {
                System.Console.WriteLine(key);
            }
            var cnt = table.Count(ps);
            System.Console.WriteLine(cnt);
            System.Console.ReadLine();
        }

        public static void ListTest(Table table)
        {
            
            
            // create value
           

        }

        public static String GetString(uint length)
        {
            Random rnd = new System.Random();
            var s = "";
            while (s.Length < length)
                s += "" + rnd.Next();
            return s;
        }
    }
}
