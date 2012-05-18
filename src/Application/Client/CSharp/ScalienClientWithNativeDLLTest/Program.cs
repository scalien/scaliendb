using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Scalien
{
    class Program
    {
        public static void Main(string[] args)
        {
            NativeLoader.Load();
            new ScalienClientUnitTesting.ConsistencyTests().CheckConfigStateShardConsistency();
            string[] nodes = { "192.168.137.100:7080", "192.168.137.101:7080", "192.168.137.102:7080" };
            var client = new Client(nodes);
            Database db = client.GetDatabase("test");
            Table table = db.GetTable("test");
        }
    }
}
