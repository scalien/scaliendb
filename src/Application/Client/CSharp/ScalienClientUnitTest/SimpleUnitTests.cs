using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;

using Scalien;

namespace ScalienClientUnitTesting
{
    [TestClass]
    class SimpleUnitTests
    {
        //[TestMethod]
        public void SetGetMP3()
        {
            string dbName = "test_mp3_db";
            string tableName = "test_mp3_table";

            Client client = new Client(Config.GetNodes());
            Utils.DeleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tbl = db.CreateTable(tableName);

            // SET MP3 (you will need a valdi path to test.mp3
            byte[] payload = Utils.ReadFile("f:/test.mp3");
            System.Console.WriteLine("mp3 buffer: {0}", payload.GetLength(0));
            tbl.Set(System.Text.Encoding.UTF8.GetBytes("mp3"), payload);

            client.Submit();

            byte[] res = tbl.Get(System.Text.Encoding.UTF8.GetBytes("mp3"));
            System.Console.WriteLine("mp3 buffer: {0}", res.GetLength(0));

            Assert.IsTrue(Utils.ByteArraysEqual(payload, res));
        }

        //[TestMethod]
        public void GetSetSubmit()
        {
            string dbName = "get_set_db";
            string tableName = "get_set_db_table";

            Client client = new Client(Config.GetNodes());
            Utils.DeleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tbl = db.CreateTable(tableName);

            tbl.TruncateTable();

            //client.Submit();

            tbl.Get("0000000000001");
            tbl.Set("0000000000001", "test");
            tbl.Get("0000000000002");
            tbl.Set("0000000000002", "test");

            client.Submit();

            var i = tbl.Count(new ByteRangeParams());
            Assert.IsTrue(i == 2);
        }

        //[TestMethod]
        public void CreateAndCloseClients()
        {
            string dbName = "create_and_close_clients_db";
            string tableName = "create_and_close_clients_table";

            Client client1 = new Client(Config.GetNodes());
            Client client2 = new Client(Config.GetNodes());

            Utils.DeleteDBs(client1);

            Database db1 = client1.CreateDatabase(dbName);
            Table tbl1 = db1.CreateTable(tableName);

            Database db2 = client1.CreateDatabase(dbName + "_2");
            Table tbl2 = db2.CreateTable(tableName);

            tbl1.TruncateTable();

            //client.Submit();

            tbl1.Get("0000000000001");
            tbl1.Set("0000000000001", "test");
            tbl1.Get("0000000000002");
            tbl1.Set("0000000000002", "test");

            client1.Submit();

            tbl1.Get("0000000000001");
            tbl1.Set("0000000000001", "test");
            tbl1.Get("0000000000002");
            tbl1.Set("0000000000002", "test");

            client1.Submit();

            tbl2.Get("0000000000001");
            tbl2.Set("0000000000001", "test");
            tbl2.Get("0000000000002");
            tbl2.Set("0000000000002", "test");

            //client1.Close();
            //client2.Close();
        }

        //[TestMethod]
        public void TruncateAfterSet()
        {
            string dbName = "get_set_db";
            string tableName = "get_set_db_table";
            Client.SetLogFile("f:\\log.txt");
            Client.SetTrace(true);
            Client client = new Client(Config.GetNodes());
            Utils.DeleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tbl = db.CreateTable(tableName);

            try
            {

                tbl.Set("0000000000001", "test");
                tbl.Set("0000000000002", "test");

                tbl.TruncateTable();

                client.Submit();

                Assert.Throw("No SDBPException!");
            }
            catch (SDBPException)
            {
            }

            //client.Close();
        }

        //[TestMethod]
        public void SequenceResetAndTruncate()
        {
            string dbName = "seq_and_trunc_db";
            string tableNameSeq = "seq_table";
            string tableNameTrunc = "trunc_table";

            Client client = new Client(Config.GetNodes());
            
            Utils.DeleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tblSeq = db.CreateTable(tableNameSeq);
            Table tblTrunc = db.CreateTable(tableNameTrunc);

            Sequence testSeq = tblSeq.GetSequence("seqIDs");

            try
            {
                testSeq.Reset();

                tblTrunc.TruncateTable();

                client.Submit();

                Assert.Throw("No SDBPException!");
            }
            catch (SDBPException)
            {
            }

            //client.Close();
        }

        //[TestMethod]
        public void SequenceAddAfterSet()
        {
            string dbName = "seq_and_set_db";
            string tableNameSeq = "seq_table";
            string tableNameSet = "set_table";

            Client client = new Client(Config.GetNodes());

            Utils.DeleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tblSeq = db.CreateTable(tableNameSeq);
            Table tblSet = db.CreateTable(tableNameSet);

            Sequence testSeq = tblSeq.GetSequence("seqIDs");

            tblSet.Set("0000000000001", "test");
            tblSet.Set("0000000000002", "test");
            tblSet.Set("0000000000003", "test");

            Console.WriteLine("Next: " + testSeq.GetNext);

            client.Submit();

            client.Close();
        }

        //[TestMethod]
        public void CountBeforeSubmit()
        {
            string dbName = "test_db";
            string tableName = "test_table";

            Client client = new Client(Config.GetNodes());

            Utils.DeleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tbl = db.CreateTable(tableName);

            for(int i = 0; i < 1000; i++)
                tbl.Set(Utils.Id(i), "test");

            var cnt1 = tbl.Count(new ByteRangeParams());

            client.Submit();

            var cnt2 = tbl.Count(new StringRangeParams());

            Console.WriteLine(cnt1 + " - " + cnt2);

            Assert.IsTrue(cnt2 == 1000);

            client.Close();
        }

        //[TestMethod]
        public void ClientInactivityTest()
        {
            Client.SetTrace(true);
            Client.SetLogFile("c:\\Users\\zszabo\\logs\\no_service_trace.txt");

            FileStream fs = new FileStream("c:\\Users\\zszabo\\logs\\no_service_console.txt", FileMode.Create);
            StreamWriter sw = new StreamWriter(fs);
            Console.SetOut(sw);

            string dbName = "test_db";
            string tableName = "test_table";

            Client client = new Client(Config.GetNodes());

            Utils.DeleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tbl = db.CreateTable(tableName);

            for (int i = 0; i < 1000; i++)
                tbl.Set(Utils.Id(i), "test");

            client.Submit();

            client.SetGlobalTimeout(30000);
            var timeout = 5 * client.GetGlobalTimeout();
            Console.WriteLine("Now sleeping for " + timeout / 1000 + " seconds");
            Thread.Sleep((int)timeout);

            foreach (string key in tbl.GetKeyIterator(new StringRangeParams()))
                Console.Write(key);
            
            client.Close();
        }
    }
}
