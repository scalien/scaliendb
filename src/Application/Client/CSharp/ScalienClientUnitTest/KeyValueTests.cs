using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Scalien;

namespace ScalienClientUnitTesting
{
    [TestClass]
    class KeyValueTests
    {
        public static bool NotLargerThan(byte[] s, byte[] l)
        {
            for (int i = 0; i < s.Length && i < l.Length; i++)
            {
                if (s[i] == l[i]) continue;

                if (s[i] < l[i]) return true;
                else return false;
            }

            if (s.Length > l.Length) return false;

            return true;
        }

        public static void sortKeyValueArrays(ref byte[][] keys, ref byte[][] values, int len)
        {
            int minid;
            byte[] tmp;

            for (int i = 0; i < len; i++)
            {
                minid = i;

                for (int j = i; j < len; j++)
                {
                    if (NotLargerThan(keys[j], keys[minid])) minid = j;
                }

                tmp = keys[minid];
                keys[minid] = keys[i];
                keys[i] = tmp;

                tmp = values[minid];
                values[minid] = values[i];
                values[i] = tmp;
            }
        }

        //[TestMethod]
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

        //[TestMethod]
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

        //[TestMethod]
        public void RandomKey_Values_CheckUsingGetByKey_FromKeyIteratorPrefix()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(Config.controllers);
            Utils.deleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tbl = db.CreateTable(tableName);
            byte[] prefix = System.Text.Encoding.UTF8.GetBytes("prefix_");

            byte[][] key = new byte[10000][];
            byte[][] value = new byte[10000][];
            
            for (int i = 0; i < 10000; i++)
            {
                key[i] = Utils.RandomASCII();
                byte[] rnd = Utils.RandomASCII();
                byte[] vl = new byte[prefix.Length + rnd.Length];
                prefix.CopyTo(vl, 0);
                rnd.CopyTo(vl, prefix.Length);
                value[i] = vl;

                tbl.Set(key[i], value[i]);
            }

            client.Submit();

            int j = 0;
            foreach (byte[] k in tbl.GetKeyIterator(new ByteRangeParams().Prefix(prefix)))
            {
                byte[] val = tbl.Get(k);
                Assert.IsNotNull(val);
                Assert.IsTrue(Utils.byteArraysEqual(value[j++], val));
            }
        }

        //[TestMethod]
        public void RandomKey_Values_CountResultsUsingKeyValueIteratorAll()
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

            sortKeyValueArrays(ref key, ref value, 10000);

            int j = 0;
            foreach (KeyValuePair<byte[], byte[]> kv in tbl.GetKeyValueIterator(new ByteRangeParams()))
            {
                j++;
            }
            Assert.IsTrue(j == 10000);
        }

        [TestMethod]
        public void RandomKey_Values_CheckUsingKeyValueIteratorAll()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(Config.controllers);
            Utils.deleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tbl = db.CreateTable(tableName);
            byte[][] key = new byte[150][];
            byte[][] value = new byte[150][];

            byte[][] key_db = new byte[170][];
            byte[][] value_db = new byte[170][];

            for (int i = 0; i < 150; i++)
            {
                key[i] = Utils.RandomASCII();
                value[i] = Utils.RandomASCII();

                tbl.Set(key[i], value[i]);
            }

            client.Submit();

            sortKeyValueArrays(ref key, ref value, 150);

            int j = 0;
            foreach (KeyValuePair<byte[], byte[]> kv in tbl.GetKeyValueIterator(new ByteRangeParams()))
            {
                key_db[j] = kv.Key;
                value_db[j++] = kv.Value;
                /*
                Assert.IsNotNull(kv);
                if (Utils.byteArraysEqual(key[j], kv.Key) == false)
                    continue;
                Assert.IsTrue(Utils.byteArraysEqual(key[j], kv.Key));
                Assert.IsTrue(Utils.byteArraysEqual(value[j++], kv.Value));*/
            }
        }

        //[TestMethod]
        public void RandomKey_Values_CheckUsingKeyValueIteratorPrefix()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(Config.controllers);
            Utils.deleteDBs(client);

            Database db = client.CreateDatabase(dbName);
            Table tbl = db.CreateTable(tableName);
            byte[] prefix = System.Text.Encoding.UTF8.GetBytes("prefix_");
            byte[][] key = new byte[10000][];
            byte[][] value = new byte[10000][];
            
            for (int i = 0; i < 10000; i++)
            {
                key[i] = Utils.RandomASCII();
                byte[] rnd = Utils.RandomASCII();
                byte[] vl = new byte[prefix.Length + rnd.Length];
                prefix.CopyTo(vl, 0);
                rnd.CopyTo(vl, prefix.Length);
                value[i] = vl;

                tbl.Set(key[i], value[i]);
            }

            client.Submit();

            int j = 0;
            foreach (KeyValuePair<byte[], byte[]> kv in tbl.GetKeyValueIterator(new ByteRangeParams().Prefix(prefix)))
            {
                Assert.IsNotNull(kv);
                Assert.IsTrue(Utils.byteArraysEqual(key[j], kv.Key));
                Assert.IsTrue(Utils.byteArraysEqual(value[j++], kv.Value));
            }
        }
    }
}
