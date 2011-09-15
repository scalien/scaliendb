using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using Scalien;

namespace TestClientUnitTest
{
    [TestClass]
    public class TestClientUnitTest
    {
        static string[] controllers_conf = { "192.168.137.50:37080", "192.168.137.51:37080", "192.168.137.52:37080" };

        [TestMethod]
        public void SetGetMP3()
        {
            string dbName = "test_mp3_db";
            string tableName = "test_mp3_table";

            Client client = new Client(controllers_conf);
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

        [TestMethod]
        public void GetSetSubmit()
        {
            string dbName = "get_set_db";
            string tableName = "get_set_db_table";

            Client client = new Client(controllers_conf);
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

        [TestMethod]
        public void TruncateAfterSet()
        {
            string dbName = "get_set_db";
            string tableName = "get_set_db_table";

            Client client = new Client(controllers_conf);
            Utils.deleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tbl = db.CreateTable(tableName);

            tbl.Set("0000000000001", "test");
            tbl.Set("0000000000002", "test");

            tbl.TruncateTable();

            client.Submit();

            var i = tbl.Count(new ByteRangeParams());
            Assert.IsTrue(i == 0);
        }

        [TestMethod]
        public void GetSetSubmitUsers()
        {       
            System.Console.WriteLine("TestingGetSetSubmit");

            Users uTest = new Users(controllers_conf);
            uTest.init();
            uTest.resetTables();

            Assert.IsTrue(uTest.testGetSetSubmit());
        }

        [TestMethod]
        public void SequenceResetAndTruncate()
        {
            string dbName = "seq_and_trunc_db";
            string tableNameSeq = "seq_table";
            string tableNameTrunc = "trunc_table";

            Client client = new Client(controllers_conf);
            Utils.deleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tblSeq = db.CreateTable(tableNameSeq);
            Table tblTrunc = db.CreateTable(tableNameTrunc);

            Sequence testSeq = tblSeq.GetSequence("seqIDs");

            testSeq.Reset();

            tblTrunc.TruncateTable();

            client.Submit();
        }
        
/*
        [TestMethod]
        public void ManyDBs100()
        {
            string dbName = "manydb_test_";
            Client client = new Client(controllers_conf);

            Utils.deleteDBs(client);

            Database[] db = new Database[100];
            
            for (int i = 0; i < 100; i++)
                client.CreateDatabase(dbName+i.ToString());

            for (int i = 0; i < 100; i++)
            {
                db[i] = client.GetDatabase(dbName + i.ToString());
                Assert.IsNotNull(db[i]);
            }
        }

        [TestMethod]
        public void ManyDBs1000()
        {
            string dbName = "manydb_test_";

            Client client = new Client(controllers_conf);

            Utils.deleteDBs(client);

            Database[] db = new Database[1000];

            for (int i = 0; i < 1000; i++)
                client.CreateDatabase(dbName + i.ToString());

            for (int i = 0; i < 1000; i++)
            {
                db[i] = client.GetDatabase(dbName + i.ToString());
                Assert.IsNotNull(db[i]);
            }
        }

        [TestMethod]
        public void ManyTables1000()
        {
            string dbName = "manytables_test";
            string tableName = "manytables_test_";

            Client client = new Client(controllers_conf);

            Utils.deleteDBs(client);

            Database db = client.CreateDatabase(dbName);

            Table[] tbl = new Table[1000];

            for (int i = 0; i < 1000; i++)
                db.CreateTable(tableName + i.ToString());

            for (int i = 0; i < 1000; i++)
            {
                tbl[i] = db.GetTable(tableName + i.ToString());
                Assert.IsNotNull(tbl[i]);
            }
        }

        [TestMethod]
        public void ManyTables10000()
        {
            string dbName = "manytables_test";
            string tableName = "manytables_test_";

            Client client = new Client(controllers_conf);

            Utils.deleteDBs(client);

            Database db = client.CreateDatabase(dbName);

            Table[] tbl = new Table[10000];

            for (int i = 0; i < 10000; i++)
                db.CreateTable(tableName + i.ToString());

            for (int i = 0; i < 10000; i++)
            {
                tbl[i] = db.GetTable(tableName + i.ToString());
                Assert.IsNotNull(tbl[i]);
            }
        }

        [TestMethod]
        public void ManyDB_AND_Tables_100x100()
        {
            string dbName = "manydbtables_test_";
            string tableName = "manydbtables_test_";

            Client client = new Client(controllers_conf);

            Utils.deleteDBs(client);

            Database[] db = new Database[100];
            Table[,] tbl = new Table[100,100];

            for (int i = 0; i < 100; i++)
                client.CreateDatabase(dbName + i.ToString());

            for (int i = 0; i < 100; i++)
            {
                db[i] = client.GetDatabase(dbName + i.ToString());
                Assert.IsNotNull(db[i]);

                // create tables in db
                for (int j = 0; j < 100; j++)
                    db[i].CreateTable(tableName + i.ToString() + "_" + j.ToString());

                for (int j = 0; j < 100; j++)
                {
                    tbl[i, j] = db[i].GetTable(tableName + i.ToString() + "_" + j.ToString());
                    Assert.IsNotNull(tbl[i,j]);
                }
            }
        }
 
        [TestMethod]
        public void RandomNamedDBs()
        {
            Client client = new Client(controllers_conf);

            Utils.deleteDBs(client);

            Database[] db = new Database[100];
            string[] dbNames = new string[100];

            for (int i = 0; i < 100; i++)
            {
                dbNames[i] = Utils.RandomString();
                client.CreateDatabase(dbNames[i]);
            }

            for (int i = 0; i < 100; i++)
            {
                db[i] = client.GetDatabase(dbNames[i]);
                Assert.IsNotNull(db[i]);
            }
        }

        [TestMethod]
        public void RandomNamedTables()
        {
            string dbName = "random_named_tables_db";
            string[] tableNames = new string[100];

            Client client = new Client(controllers_conf);

            Utils.deleteDBs(client);

            Database db = client.CreateDatabase(dbName);

            Table[] tbl = new Table[100];

            for (int i = 0; i < 100; i++)
            {
                tableNames[i] = Utils.RandomString();
                db.CreateTable(tableNames[i]);
            }

            for (int i = 0; i < 100; i++)
            {
                tbl[i] = db.GetTable(tableNames[i]);
                Assert.IsNotNull(tbl[i]);
            }
        }

        [TestMethod]
        public void RenameDBs()
        {
            string dbName = "rename_db_";
            Client client = new Client(controllers_conf);

            Utils.deleteDBs(client);

            Database[] db = new Database[100];
            string[] dbNames = new string[100];

            for (int i = 0; i < 100; i++)
            {
                dbNames[i] = dbName + i.ToString();
                client.CreateDatabase(dbNames[i]);
            }

            for (int i = 0; i < 100; i++)
            {
                db[i] = client.GetDatabase(dbNames[i]);
                Assert.IsNotNull(db[i]);

                dbNames[i] = dbName + i.ToString() + "_renamed";
                db[i].RenameDatabase(dbNames[i]);
            }

            for (int i = 0; i < 100; i++)
            {
                db[i] = client.GetDatabase(dbNames[i]);
                Assert.IsNotNull(db[i]);
            }
        }

        [TestMethod]
        public void RenameTables()
        {
            string dbName = "rename_tables_db";
            string tblName = "rename_tables_table_";
            string[] tableNames = new string[100];

            Client client = new Client(controllers_conf);

            Utils.deleteDBs(client);

            Database db = client.CreateDatabase(dbName);

            Table[] tbl = new Table[100];

            for (int i = 0; i < 100; i++)
            {
                tableNames[i] = tblName + i.ToString();
                db.CreateTable(tableNames[i]);
            }

            for (int i = 0; i < 100; i++)
            {
                tbl[i] = db.GetTable(tableNames[i]);
                Assert.IsNotNull(tbl[i]);

                tableNames[i] = tblName + i.ToString() + "_renamed";
                tbl[i].RenameTable(tableNames[i]);
            }

            for (int i = 0; i < 100; i++)
            {
                tbl[i] = db.GetTable(tableNames[i]);
                Assert.IsNotNull(tbl[i]);
            }
        }

        [TestMethod]
        public void RenameRandomNamedTables()
        {
            string dbName = "random_named_tables_db";
            string[] tableNames = new string[100];

            Client client = new Client(controllers_conf);

            Utils.deleteDBs(client);

            Database db = client.CreateDatabase(dbName);

            Table[] tbl = new Table[100];

            for (int i = 0; i < 100; i++)
            {
                tableNames[i] = Utils.RandomString();
                db.CreateTable(tableNames[i]);
            }

            for (int i = 0; i < 100; i++)
            {
                tbl[i] = db.GetTable(tableNames[i]);
                Assert.IsNotNull(tbl[i]);

                tableNames[i] = Utils.RandomString();
                tbl[i].RenameTable(tableNames[i]);
            }

            for (int i = 0; i < 100; i++)
            {
                tbl[i] = db.GetTable(tableNames[i]);
                Assert.IsNotNull(tbl[i]);
            }
        }
        */
    /*    [TestMethod]
        public void RandomKey_Values_CheckUsingGetByKey()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(controllers_conf);
            Utils.deleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tbl = db.CreateTable(tableName);
            byte[][] key = new byte[50000][];
            byte[][] value = new byte[50000][];
            
            for (int i = 0; i < 50000; i++)
            {
                key[i] = Utils.RandomASCII();
                value[i] = Utils.RandomASCII();

                tbl.Set(key[i], value[i]);
            }

            client.Submit();

            for (int i = 0; i < 50000; i++)
            {
                byte[] val = tbl.Get(key[i]);
                Assert.IsNotNull(val);
                Assert.IsTrue(Utils.byteArraysEqual(value[i], val));
            }
        }
/*
        [TestMethod]
        public void RandomKey_Values_CheckUsingGetByKey_FromKeyIteratorAll()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(controllers_conf);
            Utils.deleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tbl = db.CreateTable(tableName);
            byte[][] key = new byte[10000][];
            byte[][] value = new byte[10000][];
            
            for (int i = 0; i < 10000; i++)
            {
                key[i] = Utils.RandomASCII();
                value[i] = Utils.RandomASCII();

                tbl.Set(key[i], value[i]);
            }

            client.Submit();

            int j = 0;
            foreach (byte[] k in tbl.GetKeyIterator(new ByteRangeParams()))
            {
                byte[] val = tbl.Get(k);
                Assert.IsNotNull(val);
                Assert.IsTrue(Utils.byteArraysEqual(value[j++], val));
            }
        }
        /*
        [TestMethod]
        public void RandomKey_Values_CheckUsingGetByKey_FromKeyIteratorPrefix()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(controllers_conf);
            Utils.deleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tbl = db.CreateTable(tableName);
            byte[][] key = new byte[10000][];
            byte[][] value = new byte[10000][];
            // byte array!!!
            for (int i = 0; i < 10000; i++)
            {
                key[i] = Utils.RandomASCII();
                value[i] = Utils.RandomASCII();

                tbl.Set(key[i], value[i]);
            }

            client.Submit();
            //TODO
        }

        [TestMethod]
        public void RandomKey_Values_CheckUsingValueIteratorAll()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(controllers_conf);
            Utils.deleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tbl = db.CreateTable(tableName);
            byte[][] key = new byte[10000][];
            byte[][] value = new byte[10000][];
            // byte array!!!
            for (int i = 0; i < 10000; i++)
            {
                key[i] = Utils.RandomASCII();
                value[i] = Utils.RandomASCII();

                tbl.Set(key[i], value[i]);
            }

            client.Submit();
            //TODO
        }

        [TestMethod]
        public void RandomKey_Values_CheckUsingValueIteratorPrefix()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(controllers_conf);
            Utils.deleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tbl = db.CreateTable(tableName);
            byte[][] key = new byte[10000][];
            byte[][] value = new byte[10000][];
            // byte array!!!
            for (int i = 0; i < 10000; i++)
            {
                key[i] = Utils.RandomASCII();
                value[i] = Utils.RandomASCII();

                tbl.Set(key[i], value[i]);
            }

            client.Submit();
            //TODO
        }
*/
        [TestMethod]
        public void TruncateTables()
        {
            // create tables
            // insert data into them
            // truncate them
            // check 
            // TODO
        }
    }
}
