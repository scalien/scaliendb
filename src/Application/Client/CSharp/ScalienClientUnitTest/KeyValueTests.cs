using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Scalien;

namespace ScalienClientUnitTesting
{
    //[TestClass]
    class KeyValueTests
    {
        [TestMethod]
        public void RandomKey_Values_CheckUsingGetByKey()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(Config.controllers);
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

        [TestMethod]
        public void RandomKey_Values_CheckUsingGetByKey_FromKeyIteratorAll()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(Config.controllers);
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

        [TestMethod]
        public void RandomKey_Values_CheckUsingGetByKey_FromKeyIteratorPrefix()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(Config.controllers);
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

            // todo
            int j = 0;
            foreach (byte[] k in tbl.GetKeyIterator(new ByteRangeParams()))
            {
                byte[] val = tbl.Get(k);
                Assert.IsNotNull(val);
                Assert.IsTrue(Utils.byteArraysEqual(value[j++], val));
            }
        }

        [TestMethod]
        public void RandomKey_Values_CheckUsingValueIteratorAll()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(Config.controllers);
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

            Client client = new Client(Config.controllers);
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
    }
}
