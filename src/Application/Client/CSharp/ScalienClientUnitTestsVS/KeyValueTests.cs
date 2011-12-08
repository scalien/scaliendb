using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

#if !SCALIEN_UNIT_TEST_FRAMEWORK
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

using Scalien;

namespace ScalienClientUnitTesting
{
    [TestClass]
    public class KeyValueTests
    {
        [TestMethod]
        public void EmptyByteArrayKey()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(Config.GetNodes());
            Table tbl = Utils.GetOrCreateEmptyTableAndDatabase(client, dbName, tableName);

            byte[] key = new byte[0];
            try
            {
                tbl.Set(key, key);
                Assert.Fail("Empty key");
            }
            catch (SDBPException e)
            {
                if (e.Status != Status.SDBP_API_ERROR)
                    Assert.Fail("Empty key");
            }
        }

        [TestMethod]
        public void EmptyStringKey()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(Config.GetNodes());
            Table tbl = Utils.GetOrCreateEmptyTableAndDatabase(client, dbName, tableName);

            tbl.Set("", "");

            client.Submit();
        }

        [TestMethod]
        public void EmptyStringValue()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(Config.GetNodes());
            Table tbl = Utils.GetOrCreateEmptyTableAndDatabase(client, dbName, tableName);

            tbl.Set("empty", "");

            client.Submit();

            String value = tbl.Get("empty");
            Assert.IsTrue(value == "");
        }


        [TestMethod]
        public void RandomKey_Values_CheckUsingGetByKey()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(Config.GetNodes());
            Table tbl = Utils.GetOrCreateEmptyTableAndDatabase(client, dbName, tableName);
            byte[][] key = new byte[50000][];
            byte[][] value = new byte[50000][];
            
            for (int i = 0; i < 50000; i++)
            {
                key[i] = Utils.RandomASCII(0, true);
                value[i] = Utils.RandomASCII();

                tbl.Set(key[i], value[i]);
            }

            client.Submit();

            for (int i = 0; i < 50000; i++)
            {
                byte[] val = tbl.Get(key[i]);
                Assert.IsNotNull(val);
                Assert.IsTrue(Utils.ByteArraysEqual(value[i], val));
            }
        }

        [TestMethod]
        public void RandomKey_Values_CheckUsingGetByKey_FromKeyIteratorAll()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(Config.GetNodes());
            Table tbl = Utils.GetOrCreateEmptyTableAndDatabase(client, dbName, tableName);
            byte[][] key = new byte[10000][];
            byte[][] value = new byte[10000][];

            for (int i = 0; i < 10000; i++)
            {
                key[i] = Utils.RandomASCII(0, true);
                value[i] = Utils.RandomASCII();

                tbl.Set(key[i], value[i]);
            }

            client.Submit();

            Utils.SortKeyValueArrays(ref key, ref value, 10000);

            int j = 0;
            foreach (byte[] k in tbl.GetKeyIterator(new ByteRangeParams()))
            {
                byte[] val = tbl.Get(k);
                Assert.IsNotNull(val);
                Assert.IsTrue(Utils.ByteArraysEqual(value[j++], val));
            }
        }

        [TestMethod]
        public void RandomKey_Values_CheckUsingGetByKey_FromKeyIteratorPrefix()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(Config.GetNodes());
            Table tbl = Utils.GetOrCreateEmptyTableAndDatabase(client, dbName, tableName);
            byte[] prefix = System.Text.Encoding.UTF8.GetBytes("prefix_");

            byte[][] key = new byte[10000][];
            byte[][] value = new byte[10000][];
            
            for (int i = 0; i < 10000; i++)
            {
                byte[] rnd = Utils.RandomASCII(0, true);
                byte[] ky = new byte[prefix.Length + rnd.Length];
                prefix.CopyTo(ky, 0);
                rnd.CopyTo(ky, prefix.Length);
                key[i] = ky;

                value[i] = Utils.RandomASCII();

                tbl.Set(key[i], value[i]);
            }

            client.Submit();

            Utils.SortKeyValueArrays(ref key, ref value, 10000);

            int j = 0;
            foreach (byte[] k in tbl.GetKeyIterator(new ByteRangeParams().Prefix(prefix)))
            {
                byte[] val = tbl.Get(k);
                Assert.IsNotNull(val);
                Assert.IsTrue(Utils.ByteArraysEqual(value[j++], val));
            }
        }

        [TestMethod]
        public void RandomKey_Values_CountResultsUsingKeyValueIteratorAll()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(Config.GetNodes());
            Table tbl = Utils.GetOrCreateEmptyTableAndDatabase(client, dbName, tableName);
            byte[][] key = new byte[10000][];
            byte[][] value = new byte[10000][];

            for (int i = 0; i < 10000; i++)
            {
                key[i] = Utils.RandomASCII(0, true);
                value[i] = Utils.RandomASCII();

                tbl.Set(key[i], value[i]);
            }

            client.Submit();

            Utils.SortKeyValueArrays(ref key, ref value, 10000);

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

            Client client = new Client(Config.GetNodes());
            Table tbl = Utils.GetOrCreateEmptyTableAndDatabase(client, dbName, tableName);
            byte[][] key = new byte[1500][];
            byte[][] value = new byte[1500][];

            byte[][] key_db = new byte[1500][];
            byte[][] value_db = new byte[1500][];

            for (int i = 0; i < 1500; i++)
            {
                key[i] = Utils.RandomASCII(0,true);
                value[i] = Utils.RandomASCII();

                tbl.Set(key[i], value[i]);
            }

            client.Submit();

            Utils.SortKeyValueArrays(ref key, ref value, 1500);

            int j = 0;
            foreach (KeyValuePair<byte[], byte[]> kv in tbl.GetKeyValueIterator(new ByteRangeParams()))
            {
                key_db[j] = kv.Key;
                value_db[j] = kv.Value;
                
                Assert.IsNotNull(kv);
                Assert.IsTrue(Utils.ByteArraysEqual(key[j], kv.Key));
                Assert.IsTrue(Utils.ByteArraysEqual(value[j++], kv.Value));
            }
        }

        [TestMethod]
        public void RandomKey_Values_CheckUsingKeyValueIteratorPrefix()
        {
            string dbName = "random_key_values_db";
            string tableName = "random_key_values_tbl";

            Client client = new Client(Config.GetNodes());
            Table tbl = Utils.GetOrCreateEmptyTableAndDatabase(client, dbName, tableName);
            byte[] prefix = System.Text.Encoding.UTF8.GetBytes("prefix_");
            byte[][] key = new byte[10000][];
            byte[][] value = new byte[10000][];
            
            for (int i = 0; i < 10000; i++)
            {
                byte[] rnd = Utils.RandomASCII(0, true);
                byte[] ky = new byte[prefix.Length + rnd.Length];
                prefix.CopyTo(ky, 0);
                rnd.CopyTo(ky, prefix.Length);
                key[i] = ky;

                value[i] = Utils.RandomASCII();

                tbl.Set(key[i], value[i]);
            }

            client.Submit();

            Utils.SortKeyValueArrays(ref key, ref value, 10000);

            int j = 0;
            foreach (KeyValuePair<byte[], byte[]> kv in tbl.GetKeyValueIterator(new ByteRangeParams().Prefix(prefix)))
            {
                Assert.IsNotNull(kv);
                Assert.IsTrue(Utils.ByteArraysEqual(key[j], kv.Key));
                Assert.IsTrue(Utils.ByteArraysEqual(value[j++], kv.Value));
            }
        }
    }
}
