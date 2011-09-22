using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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

            Client client = new Client(Config.controllers);
            Utils.deleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tbl = db.CreateTable(tableName);

            // SET MP3 (you will need a valdi path to test.mp3
            byte[] payload = Utils.ReadFile("f:/test.mp3");
            System.Console.WriteLine("mp3 buffer: {0}", payload.GetLength(0));
            tbl.Set(System.Text.Encoding.UTF8.GetBytes("mp3"), payload);

            client.Submit();

            byte[] res = tbl.Get(System.Text.Encoding.UTF8.GetBytes("mp3"));
            System.Console.WriteLine("mp3 buffer: {0}", res.GetLength(0));

            Assert.IsTrue(Utils.byteArraysEqual(payload, res));
        }

        //[TestMethod]
        public void GetSetSubmit()
        {
            string dbName = "get_set_db";
            string tableName = "get_set_db_table";

            Client client = new Client(Config.controllers);
            Utils.deleteDBs(client);

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

            Client client1 = new Client(Config.controllers);
            Client client2 = new Client(Config.controllers);

            Utils.deleteDBs(client1);

            Database db = client1.CreateDatabase(dbName);
            Table tbl = db.CreateTable(tableName);

            tbl.TruncateTable();

            //client.Submit();

            tbl.Get("0000000000001");
            tbl.Set("0000000000001", "test");
            tbl.Get("0000000000002");
            tbl.Set("0000000000002", "test");

            client1.Submit();

            //client1.Close();
            //client2.Close();
        }

        [TestMethod]
        public void TruncateAfterSet()
        {
            string dbName = "get_set_db";
            string tableName = "get_set_db_table";
            Client.SetLogFile("f:\\log.txt");
            Client.SetTrace(true);
            Client client = new Client(Config.controllers);
            Utils.deleteDBs(client);

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

        [TestMethod]
        public void SequenceResetAndTruncate()
        {
            string dbName = "seq_and_trunc_db";
            string tableNameSeq = "seq_table";
            string tableNameTrunc = "trunc_table";

            Client client = new Client(Config.controllers);
            
            Utils.deleteDBs(client);

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

            client.Close();
        }

    }
}
