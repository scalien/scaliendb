using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;

#if !SCALIEN_UNIT_TEST_FRAMEWORK
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

using Scalien;

namespace ScalienClientUnitTesting
{
    [TestClass]
    public class UsersTests
    {
        [TestMethod]
        public void CountUsers() // for debug only
        {
            Users usr = new Users(Config.GetNodes());

            long cnt = usr.CountUsers();

            Console.WriteLine("Count is " + cnt);
        }

        [TestMethod]
        public void SimpleTest()
        {
            Users usr = new Users(Config.GetNodes());
            usr.EmptyAll();

            usr.InsertUsers(1000);
            // bug ?
            long cnt = usr.CountUsers();
            usr.SubmitAll();
            usr.TestCycle(500);
            
            Assert.IsTrue(usr.IsConsistent());
        }

        private static void TestWorker(Object param)
        {
            int loop = System.Convert.ToInt32(param);
            int users_per_iteration = 2;

            Users usr = new Users(Config.GetNodes());
            while (loop-- > 0)
            {
                usr.TestCycle(users_per_iteration);
            }
        }

        [TestMethod]
        public void ShortTest_10_Threads()
        {
            /*Client.SetTrace(true);
            Client.SetLogFile("c:\\Users\\zszabo\\logs\\client_trace.txt");*/
            int init_users = 10000;
            int threadnum = 10;

            /*FileStream fs = new FileStream("c:\\Users\\zszabo\\logs\\threadout_10.txt", FileMode.Create);
            StreamWriter sw = new StreamWriter(fs);
            Console.SetOut(sw);*/

            Users usr = new Users(Config.GetNodes());
            usr.EmptyAll();
            usr.InsertUsers(init_users);

            Thread[] threads = new Thread[threadnum];
            for (int i = 0; i < threadnum; i++)
            {
                threads[i] = new Thread(new ParameterizedThreadStart(TestWorker));
                threads[i].Start(500);
            }

            for (int i = 0; i < threadnum; i++)
            {
                threads[i].Join();
            }

            Assert.IsTrue(usr.IsConsistent());
        }

        [TestMethod]
        public void ShortTest_100_Threads()
        {
            /*Client.SetTrace(true);
            Client.SetLogFile("c:\\Users\\zszabo\\logs\\client_trace_100.txt");*/
            int init_users = 50000;
            int threadnum = 100;
            /*
            FileStream fs = new FileStream("c:\\Users\\zszabo\\logs\\threadout_100.txt", FileMode.Create);
            StreamWriter sw = new StreamWriter(fs);
            Console.SetOut(sw);*/

            Users usr = new Users(Config.GetNodes());
            usr.EmptyAll();
            usr.InsertUsers(init_users);

            Thread[] threads = new Thread[threadnum];
            for (int i = 0; i < threadnum; i++)
            {
                threads[i] = new Thread(new ParameterizedThreadStart(TestWorker));
                threads[i].Start(500);
            }

            for (int i = 0; i < threadnum; i++)
            {
                threads[i].Join();
            }

            Console.WriteLine("Checking consistency");

            Assert.IsTrue(usr.IsConsistent());
        }

        //[TestMethod]
        public void ShortTest_1000_Threads()
        {
            int init_users = 1000;
            int threadnum = 1000;

            Users usr = new Users(Config.GetNodes());
            usr.EmptyAll();
            usr.InsertUsers(init_users);

            Thread[] threads = new Thread[threadnum];
            for (int i = 0; i < threadnum; i++)
            {
                threads[i] = new Thread(new ParameterizedThreadStart(TestWorker));
                threads[i].Start(500);
            }

            for (int i = 0; i < threadnum; i++)
            {
                threads[i].Join();
            }

            Assert.IsTrue(usr.IsConsistent());
        }

        public void LongTest_10_Threads()
        {
        }

        public void LongTest_100_Threads()
        {
        }

        public void LongTest_1000_Threads()
        {
        }
    }
}
