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
            for (var i = 0; i < 1; i++)
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
            string[] controllers = { "192.168.137.100:7080", "192.168.137.101:7080", "192.168.137.102:7080" };
            Client client = new Client(controllers);

            Database db = client.GetDatabase("test");
            Table table = db.GetTable("test");

            System.Console.WriteLine("Thread.CurrentThread.ManagedThreadId = {0}", Thread.CurrentThread.ManagedThreadId);

            System.Random random = new System.Random(Thread.CurrentThread.ManagedThreadId);
            var value = "";
            while (value.Length < 10 * 1000)
                value += "" + random.Next();

            using (client.Begin())
            {
                for (var i = 0; i < 300 * 1000; i++)
                {
                    var key = "" + random.Next(2);
                    key += random.Next();
                    table.Set(key, value);
                }
            }
        }
    }
}
