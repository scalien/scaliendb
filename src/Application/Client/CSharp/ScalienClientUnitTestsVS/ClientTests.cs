using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;

#if !SCALIEN_UNIT_TEST_FRAMEWORK
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

using Scalien;
using System.Threading;

namespace ScalienClientUnitTesting
{
    /// <summary>
    /// Summary description for ClientTest
    /// </summary>
    [TestClass]
    public class ClientTests
    {
        public ClientTests()
        {
            NativeLoader.Load();
        }

        [TestMethod]
        public void CreateClientPerRequest()
        {
            var dbName = "test";
            var tableName = "CreateClientPerRequest";

            // make sure the db and table exists
            Assert.IsNotNull(Utils.GetOrCreateTableAndDatabase(new Client(Utils.GetConfigNodes()), dbName, tableName));

            // without connection pooling
            Client.SetConnectionPoolSize(0);
            for (var i = 0; i < 100 * 1000; i++)
            {
                Client client = new Client(Utils.GetConfigNodes());
                Database db = client.GetDatabase(dbName);
                Table table = db.GetTable(tableName);
                table.Set("" + i, "" + i);
            }

            // with connection pooling
            Client.SetConnectionPoolSize(100);
            for (var i = 0; i < 100 * 1000; i++)
            {
                Client client = new Client(Utils.GetConfigNodes());
                Database db = client.GetDatabase(dbName);
                Table table = db.GetTable(tableName);
                table.Set("" + i, "" + i);
            }
        }
        
        [TestMethod]
        public void TestMaxConnections()
        {
            var dbName = "test";
            var tableName = "CreateClientPerRequest";

            // make sure the db and table exists
            Assert.IsNotNull(Utils.GetOrCreateTableAndDatabase(new Client(Utils.GetConfigNodes()), dbName, tableName));

            Client.SetMaxConnections(10);

            for (var i = 0; i < 11; i++)
            {
                Client client = new Client(Utils.GetConfigNodes());
                client.SetGlobalTimeout(15000);
                client.SetMasterTimeout(10000);
                Database db = client.GetDatabase(dbName);
                Table table = db.GetTable(tableName);
                try
                {
                    table.Set("" + i, "" + i);
                    client.Submit();
                }
                catch (SDBPException e)
                {
                    Assert.IsTrue(i == 10 && e.Status == Status.SDBP_FAILURE);
                }
            }
        }

        private bool isMultiThreadedClientRunning = false;
        private void MultiThreadedClientThreadFunc()
        {
            var dbName = "test";
            var tableName = "MultiThreadedClient";

            Random rnd = new Random();
            Client client = null;
            Database db = null;
            Table table = null;

            Utils.GetOrCreateTableAndDatabase(new Client(Utils.GetConfigNodes()), dbName, tableName);

            while (isMultiThreadedClientRunning)
            {
                string key = "" + rnd.Next(10000);
                //Thread.Sleep(10);

                try
                {

                    switch (rnd.Next(4))
                    {
                        case 0:
                            client = new Client(Utils.GetConfigNodes());
                            client.SetGlobalTimeout(15 * 1000);
                            db = client.GetDatabase(dbName);
                            table = db.GetTable(tableName);
                            break;
                        case 1:
                            client = null;
                            Thread.Sleep(100);
                            break;
                        case 2:
                            if (client != null)
                            {
                                string value = table.Get(key);
                                if (value != null)
                                {
                                    try
                                    {
                                        long.Parse(value);
                                    }
                                    catch (FormatException e)
                                    {
                                        Console.WriteLine(e.Message);
                                        
                                    }
                                }
                            }
                            break;
                        case 3:
                            if (client != null)
                            {
                                table.Add(key, 1);
                            }
                            break;
                    }
                }
                catch (SDBPException e)
                {
                    Console.WriteLine(e.Message);
                    Assert.Fail(e.Message);
                }
            }
        }

        [TestMethod]
        public void TestMultithreadedClient()
        {
            int testDuration = 600; // in seconds
            int numThreads = 100;
            Thread[] threads = new Thread[numThreads];

            Client.SetLogFile("clientlog.txt");
            Client.SetDebug(true);
            Client.SetConnectionPoolSize(100);
            Client.SetMaxConnections(200);

            isMultiThreadedClientRunning = true;

            for (var i = 0; i < numThreads; i++)
            {
                threads[i] = new Thread(new ThreadStart(MultiThreadedClientThreadFunc));
                threads[i].Start();
            }

            Thread.Sleep(testDuration * 1000);
            isMultiThreadedClientRunning = false;

            foreach (var thread in threads)
            {
                thread.Join();
            }
        }

        [TestMethod]
        public void TestNoConnectionException()
        {
            try
            {
                // bad port
                Client client = new Client(new string[] { "127.0.0.1:1" });
                client.SetMasterTimeout(5 * 1000);
                client.SetGlobalTimeout(10 * 1000);
                string configState = client.GetJSONConfigState();
            }
            catch (SDBPException e)
            {
                Assert.IsTrue(e.Status == Status.SDBP_NOCONNECTION);
                return;
            }

            Assert.Fail();
        }

        [TestMethod]
        public void TestNoMasterException()
        {
            // This test is not working, because it always throws Status.SDBP_NOCONNECTION
            try
            {
                // ensure that there is no master
                Client client = new Client(new string[] { "192.168.2.118:7080", "192.168.2.119:7080", "192.168.2.120:7080" });
                client.SetMasterTimeout(5 * 1000);
                client.SetGlobalTimeout(10 * 1000);
                string configState = client.GetJSONConfigState();
            }
            catch (SDBPException e)
            {
                Assert.IsTrue(e.Status == Status.SDBP_NOMASTER);
                return;
            }

            Assert.Fail();
        }

        [TestMethod]
        public void TestNoPrimaryException()
        {
            try
            {
                // ensure that there is no master
                Client client = new Client(new string[] { "192.168.2.118:7080", "192.168.2.119:7080", "192.168.2.120:7080" });
                client.SetMasterTimeout(5 * 1000);
                client.SetGlobalTimeout(10 * 1000);
                Client.SetTrace(true);
                string configState = client.GetJSONConfigState();
                Database db = client.GetDatabase("test_db");
                Table table = db.GetTable("test_table");
                for (int i = 0; i < 100; i++)
                {
                    table.Set("a", "1");
                    client.Submit();
                    Thread.Sleep(500);
                }
            }
            catch (SDBPException e)
            {
                Assert.IsTrue(e.Status == Status.SDBP_FAILURE);
                return;
            }

            Assert.Fail();
        }
    }
}
