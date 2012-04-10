using System;
using System.IO;
using System.Collections.Generic;
using System.Runtime.Serialization.Json;
using System.Diagnostics;
using System.Threading;

namespace Scalien
{
    public class Test
    {
        public static byte[] StringToByteArray(string str)
        {
            return System.Text.Encoding.UTF8.GetBytes(str);
        }

        internal static string ByteArrayToString(byte[] data)
        {
            return System.Text.UTF8Encoding.UTF8.GetString(data);
        }

        public static void Main(string[] args)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            List<Thread> threads = new List<Thread>();
            for (var i = 0; i < 100; i++)
            {
                Thread thread = new Thread(new ThreadStart(ThreadFunc));
                thread.Start();
                threads.Add(thread);
            }

            foreach (var thread in threads)
            {
                thread.Join();
            }

            stopwatch.Stop();

            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);

            System.Console.ReadLine();
        }

        public static void ThreadFunc()
        {
            //Client.SetTrace(true);
            //Client.SetLogFile("d:/out.txt");

            //string[] controllers = { "127.0.0.1:7080" };
            string[] controllers = { "192.168.137.110:7080", "192.168.137.111:7080", "192.168.137.112:7080" };
            Client client = new Client(controllers);

            Quorum quorum = client.GetQuorum("test");
            Database db = client.GetDatabase("test");
            Table test = db.GetTable("test");
            Table indices = db.GetTable("indices");
            Sequence IDs = indices.GetSequence("IDs");

            //System.Console.WriteLine("Thread.CurrentThread.ManagedThreadId = {0}", Thread.CurrentThread.ManagedThreadId);

            System.Random random = new System.Random(Thread.CurrentThread.ManagedThreadId);

            var value = "";
            while (value.Length < 10 * 1000)
                value += "" + random.Next();

            //using (client.Transaction(quorum, "foo" + random.Next(100)))
            //{
            //    System.Console.WriteLine("Transaction started.");
            //    for (var j = 0; j < 10*1000; j++)
            //            table.Set("" + j, value);

            //    client.CommitTransaction();
            //    System.Console.WriteLine("Transaction finished.");
            //}
            //return;

            ulong ID;

            //for (var i = 0; i < 1000; i++)
            while (true)
            {
                if (random.Next(3) == 0)
                {
                    try
                    {
                        System.Console.WriteLine("Batch started.");
                        for (var j = 0; j < 10; j++)
                        {
                            if (random.Next(2) == 0)
                                test.Set("" + random.Next(1000 * 1000), value);
                            else
                                test.Delete("" + random.Next(1000 * 1000));
                        }
                        //for (var j = 0; j < 10 * 1000; j++)
                        //    ID = IDs.GetNext;
                        client.Submit();
                        System.Console.WriteLine("Batch finished.");
                    }
                    catch (SDBPException)
                    {
                        System.Console.WriteLine("Batch failed.");
                    }
                    continue;
                }

                //if (random.Next(2) == 0)
                //{
                //    try
                //    {
                //        System.Console.WriteLine("List started.");
                //        var count = 0;
                //        foreach (KeyValuePair<string, string> kv in test.GetKeyValueIterator(new StringRangeParams().Prefix("1")))
                //        {
                //            count++;
                //            //System.Console.WriteLine(kv.Key + " => " + kv.Value);
                //            if (count == 10)
                //                break;
                //        }
                //        System.Console.WriteLine("List finished.");
                //    }
                //    catch (SDBPException)
                //    {
                //        System.Console.WriteLine("List failed.");
                //        // why does this happen
                //    }
                //    continue;
                //}
                
                while (true)
                {
                    try
                    {
                        using (client.Transaction(quorum, "foo" + random.Next(100)))
                        {
                            System.Console.WriteLine("Transaction started.");
                            for (var j = 0; j < 10; j++)
                            {
                                if (random.Next(2) == 0)
                                    test.Set("" + random.Next(1000 * 1000), value);
                                else
                                    test.Delete("" + random.Next(1000 * 1000));
                            }
                            //for (var j = 0; j < 1000; j++)
                            //    ID = IDs.GetNext;
                            if (random.Next(2) == 0)
                                client.CommitTransaction();
                            else
                                client.RollbackTransaction();
                            System.Console.WriteLine("Transaction finished.");
                            break;
                        }
                    }
                    catch (SDBPException)
                    {
                        //System.Console.WriteLine("Exception.");
                        System.Threading.Thread.Sleep(1);
                    }
                }
            }
        }
    }
}
