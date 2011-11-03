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

        // test entry point
        public static void Main(string[] args)
        {
            //Client.SetTrace(true);
            //Client.SetLogFile("d:/out.txt");

            string[] controllers = { "127.0.0.1:7080" };
            Client client = new Client(controllers);

            Database db = client.GetDatabase("test");
            Table table = db.GetTable("test");

            Sequence seq = table.GetSequence("newseq");
            seq.Set(13);

            //var sw = new Stopwatch();
            //sw.Start();
            //for (var i = 0; i < 10*1000; i++)
            //{
            //    var s = seq.GetNext;
            //}
            //sw.Stop();
            //Console.WriteLine("Elapsed time: {0}", sw.Elapsed); // TimeSpan

            //System.Random random = new System.Random();
            //var value = "";
            //while (value.Length < 10 * 1000)
            //    value += "" + random.Next();

            //using (client.Begin())
            //{
            //    for (var i = 0; i < 1000 * 1000; i++)
            //    {
            //        var key = i.ToString("D9");
            //        table.Set(key, value);
            //    }
            //}
        }
    }
}
