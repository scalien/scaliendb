using System.Collections.Generic;
using System.Threading;
using System;

namespace Scalien
{
    public class Test
    {
        //static Client client;
        //static Database db;
        //static Table table;
        //static Sequence seq;

        public static void ThreadFunc()
        {
            while (true)
            {
                try
                {
                    string[] controllers = { "127.0.0.1:7080" };
                    Client client = new Client(controllers);
                    //Client.SetTrace(true);
                    //client.SetConsistencyMode(Client.CONSISTENCY_ANY);
                    Database db = client.GetDatabase("test");
                    Table table = db.GetTable("test");
                    System.Random random = new System.Random();
                    var numOps = 1000;
                    while (true)
                    {
                        using (client.Begin())
                        {
                            for (var i = 0; i < numOps; i++)
                            {
                                var r = "foo";// +random.Next();
                                table.Set(r, r);
                            }
                        }
                        System.Console.WriteLine("Done with numOps = " + numOps + " (" + random.Next() + ")");
                    }

                    client.Close();
                }
                catch (Exception e)
                {
                }
            }
        }
        
        // test entry point
        public static void Main(string[] args)
        {
            var numThreads = 1000;

            while (true)
            {
                List<Thread> threads = new List<Thread>();
                for (var i = 0; i < numThreads; i++)
                {
                    Thread t = new Thread(new ThreadStart(ThreadFunc));
                    threads.Add(t);
                }

                for (var i = 0; i < numThreads; i++)
                {
                    threads[i].Start();
                }
                
                for (var i = 0; i < numThreads; i++)
                {
                    threads[i].Join();
                }
                System.Console.WriteLine("Done.");
            }
        }
    }
}
