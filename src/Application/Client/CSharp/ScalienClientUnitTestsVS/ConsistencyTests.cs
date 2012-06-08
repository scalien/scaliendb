using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

#if !SCALIEN_UNIT_TEST_FRAMEWORK
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

using Scalien;

namespace ScalienClientUnitTesting
{
    [TestClass]
    public class ConsistencyTests
    {
        private static int COUNT_TIMEOUT = 120 * 1000;

        private static string[] GetControllersHTTPEndpoint(string[] controllers)
        {
            var httpEndpoints = new string[controllers.Length];
            for (var i = 0; i < controllers.Length; i++)
            {
                var address = controllers[i].Split(new char[] { ':' })[0];
                httpEndpoints[i] = "http://" + address + ":8080/";
            }

            return httpEndpoints;
        }

        [TestMethod]
        public void CheckConfigStateConsistency()
        {
            Console.WriteLine("\nChecking config state consistency...\n");

            var client = new Client(Utils.GetConfigNodes());
            var jsonConfigState = client.GetJSONConfigState();
            var clientConfigState = Utils.JsonDeserialize<ConfigState>(System.Text.Encoding.UTF8.GetBytes(jsonConfigState));
            var master = clientConfigState.master;
            var controllers = GetControllersHTTPEndpoint(Utils.GetConfigNodes());
            foreach (var controller in controllers)
            {
                var url = controller + "json/getconfigstate";
                jsonConfigState = Utils.HTTP.GET(url, COUNT_TIMEOUT);
                var configState = Utils.JsonDeserialize<ConfigState>(System.Text.Encoding.UTF8.GetBytes(jsonConfigState));
                Assert.IsTrue(configState.master == -1 || configState.master == master);
            }
        }

        [TestMethod]
        public void CheckConfigStateShardConsistency()
        {
            Console.WriteLine("\nChecking config state shard consistency...\n");

            var client = new Client(Utils.GetConfigNodes());
            var jsonConfigState = client.GetJSONConfigState();
            var configState = Utils.JsonDeserialize<ConfigState>(System.Text.Encoding.UTF8.GetBytes(jsonConfigState));
            foreach (var table in configState.tables)
            {
                System.Console.WriteLine("\nChecking table {0}", table.tableID);

                var shards = ConfigStateHelpers.GetTableShards(table, configState.shards);
                var shardMap = new Dictionary<string, ConfigState.Shard>();
                
                // build a map of shards by firstkey
                foreach (var shard in shards)
                {
                    ConfigState.Shard testShard;
                    Assert.IsFalse(shardMap.TryGetValue(shard.firstKey, out testShard));
                    shardMap.Add(shard.firstKey, shard);
                }

                // walk the map of shards by the last key of the shard
                var firstKey = "";
                int count = 0;
                ConfigState.Shard tmpShard;
                while (shardMap.TryGetValue(firstKey, out tmpShard))
                {
                    count += 1;
                    if (tmpShard.lastKey == "")
                        break;
                    firstKey = tmpShard.lastKey;
                }
                
                Assert.IsTrue(count == shards.Count);
            }
        }


        [TestMethod]
        public void CheckClusterConsistencyByCount()
        {
            var client = new Client(Utils.GetConfigNodes());
            var jsonConfigState = client.GetJSONConfigState();
            var configState = Utils.JsonDeserialize<ConfigState>(System.Text.Encoding.UTF8.GetBytes(jsonConfigState));
            foreach (var table in configState.tables)
            {
                var shards = ConfigStateHelpers.GetTableShards(table, configState.shards);
                foreach (var shard in shards)
                {
                    System.Console.WriteLine("\nCounting shard " + shard.shardID);

                    var tableID = shard.tableID;
                    var startKey = shard.firstKey;
                    var endKey = shard.lastKey;
                    var quorum = ConfigStateHelpers.GetQuorum(configState, shard.quorumID);
                    var shardServers = ConfigStateHelpers.GetQuorumActiveShardServers(configState, quorum);

                    ParallelTableCountHTTP(shardServers, tableID, startKey, endKey);

                    System.Console.WriteLine("Shard " + shard.shardID + " is consistent.\n");
                }
            }
        }

        [TestMethod]
        public void CheckClusterConsistencyByKeys()
        {
            var client = new Client(Utils.GetConfigNodes());
            var jsonConfigState = client.GetJSONConfigState();
            var configState = Utils.JsonDeserialize<ConfigState>(System.Text.Encoding.UTF8.GetBytes(jsonConfigState));
            foreach (var table in configState.tables)
            {
                var shards = ConfigStateHelpers.GetTableShards(table, configState.shards);
                foreach (var shard in shards)
                {
                    System.Console.WriteLine("\nComparing shard keys on shard " + shard.shardID);

                    var tableID = shard.tableID;
                    var startKey = shard.firstKey;
                    var endKey = shard.lastKey;
                    var quorum = ConfigStateHelpers.GetQuorum(configState, shard.quorumID);
                    var shardServers = ConfigStateHelpers.GetQuorumActiveShardServers(configState, quorum);

                    CompareTableKeysHTTP(shardServers, tableID, startKey, endKey);
                    
                    System.Console.WriteLine("Shard " + shard.shardID + " is consistent.\n");
                }
            }
        }

        [TestMethod]
        public void CheckClusterConsistencyByKeyValues()
        {
            var client = new Client(Utils.GetConfigNodes());
            var jsonConfigState = client.GetJSONConfigState();
            var configState = Utils.JsonDeserialize<ConfigState>(System.Text.Encoding.UTF8.GetBytes(jsonConfigState));
            foreach (var table in configState.tables)
            {
                var shards = ConfigStateHelpers.GetTableShards(table, configState.shards);
                foreach (var shard in shards)
                {
                    System.Console.WriteLine("\nComparing shard key-values on shard " + shard.shardID);

                    var tableID = shard.tableID;
                    var startKey = Utils.StringToByteArray(shard.firstKey);
                    var endKey = Utils.StringToByteArray(shard.lastKey);
                    var quorum = ConfigStateHelpers.GetQuorum(configState, shard.quorumID);
                    var shardServers = ConfigStateHelpers.GetQuorumActiveShardServers(configState, quorum);

                    CompareTableKeyValuesHTTP(shardServers, tableID, startKey, endKey);

                    System.Console.WriteLine("Shard " + shard.shardID + " is consistent.\n");
                }
            }
        }

        [TestMethod]
        public void CheckAndFixClusterConsistencyByKeys()
        {
            var client = new Client(Utils.GetConfigNodes());
            var jsonConfigState = client.GetJSONConfigState();
            var configState = Utils.JsonDeserialize<ConfigState>(System.Text.Encoding.UTF8.GetBytes(jsonConfigState));
            foreach (var table in configState.tables)
            {
                var shards = ConfigStateHelpers.GetTableShards(table, configState.shards);
                foreach (var shard in shards)
                {
                    System.Console.WriteLine("\nComparing shard keys on shard " + shard.shardID);

                    var tableID = shard.tableID;
                    var startKey = shard.firstKey;
                    var endKey = shard.lastKey;
                    var quorum = ConfigStateHelpers.GetQuorum(configState, shard.quorumID);
                    var shardServers = ConfigStateHelpers.GetQuorumActiveShardServers(configState, quorum);

                    FixConsistency(client, shardServers, tableID, startKey, endKey);

                    System.Console.WriteLine("Shard " + shard.shardID + " is consistent.\n");
                }
            }
        }

        public static void FillDatabaseWithNumericKeys(string databaseName, string tableName)
        {
            var client = new Client(Utils.GetConfigNodes());
            Assert.IsTrue(ConfigStateHelpers.TryDeleteDatabase(client, databaseName));
            var database = client.CreateDatabase(databaseName);
            Assert.IsNotNull(database, "Cannot create database " + databaseName);
            var table = ConfigStateHelpers.TryCreateTable(database, tableName);
            Assert.IsNotNull(table, "Cannot create table " + tableName);

            System.Console.WriteLine("Filling the database...");

            DateTime last = DateTime.Now;
            for (ulong i = 0; i < 100 * 1000 * 1000; i++)
            {
                var key = Utils.Id(i);
                table.Set(key, key);
                TimeSpan timeSpan = DateTime.Now - last;
                if (timeSpan.TotalSeconds >= 60)
                {
                    System.Console.WriteLine("i: " + i);
                    last = DateTime.Now;
                }
            }

            client.Submit();

            System.Console.WriteLine("Database filling is done.");
        }

        public static void ParallelTableCountHTTP(List<ConfigState.ShardServer> shardServers, Int64 tableID, string startKey, string endKey)
        {
            var threads = new CounterThreadState[shardServers.Count];

            var i = 0;
            foreach (var shardServer in shardServers)
            {
                var url = ConfigStateHelpers.GetShardServerURL(shardServer) + "count?tableID=" + tableID + "&startKey=" + startKey + "&endKey=" + endKey;
                System.Console.WriteLine("  Getting count on nodeID: " + shardServer.nodeID + ", tableID: " + tableID);

                threads[i] = new CounterThreadState();
                threads[i].thread = new Thread(new ThreadStart(threads[i].CounterThreadFunc));
                threads[i].shardServer = shardServer;
                threads[i].count = -1;
                threads[i].url = url;

                threads[i].thread.Start();

                i += 1;
            }

            Int64 prevCount = -1;
            foreach (var thread in threads)
            {
                thread.thread.Join();
                if (prevCount == -1)
                    prevCount = thread.count;
                System.Console.WriteLine("  Result on " + thread.shardServer.nodeID + ": " + thread.count + ", elapsed: " + thread.elapsed.Seconds + "s, cps: " + (Int64)(thread.count / thread.elapsed.TotalMilliseconds) * 1000);
                Assert.IsTrue(thread.count != -1 && thread.count == prevCount);
                prevCount = thread.count;
            }
        }

        public static void CompareTableKeysHTTP(List<ConfigState.ShardServer> shardServers, Int64 tableID, string startKey, string endKey)
        {
            if (shardServers.Count <= 1)
                return;

            var serverKeys = new string[shardServers.Count][];
            var i = 0;
            while (true)
            {
                serverKeys = ConfigStateHelpers.ParallelFetchTableKeysHTTP(shardServers, tableID, startKey, endKey, true);

                for (i = 1; i < serverKeys.Length; i++)
                {
                    if (!serverKeys[i].SequenceEqual(serverKeys[0]))
                    {
                        for (var j = 0; j < Math.Max(serverKeys[i].Length, serverKeys[0].Length); j++)
                        {
                            var a = serverKeys[i][j];
                            var b = serverKeys[0][j];
                            if (a == b)
                                continue;

                            System.Console.WriteLine("Inconsistency at tableID: " + tableID);
                            System.Console.WriteLine("NodeID: " + shardServers.ElementAt(i).nodeID + ", key: " + a);
                            System.Console.WriteLine("NodeID: " + shardServers.ElementAt(0).nodeID + ", key: " + b);
                            //Assert.IsTrue(a == b);
                            //return;
                            goto Out;
                        }
                    }
                }

            Out:
                if (serverKeys[0].Length <= 1)
                    break;

                startKey = serverKeys[0][serverKeys[0].Length - 1];
                System.Console.WriteLine("StartKey: " + startKey);
            }
        }

        public static void CompareTableKeyValuesHTTP(List<ConfigState.ShardServer> shardServers, Int64 tableID, byte[] startKey, byte[] endKey)
        {
            if (shardServers.Count <= 1)
                return;

            var i = 0;
            while (true)
            {
                var serverKeys = ConfigStateHelpers.ParallelFetchTableKeyValuesHTTP(shardServers, tableID, startKey, endKey, true);
                int numEmpty = serverKeys.Count(keys => keys.Count == 0);
                if (numEmpty == serverKeys.Length)
                    break;

                System.Console.WriteLine("StartKey: " + Utils.ByteArrayToString(startKey));

                for (i = 1; i < serverKeys.Length; i++)
                {
                    if (serverKeys[i].Count != serverKeys[0].Count)
                    {
                        System.Console.WriteLine("Inconsistency at tableID: " + tableID);
                        System.Console.WriteLine("NodeID: " + shardServers.ElementAt(i).nodeID + ", count: " + serverKeys[i].Count);
                        System.Console.WriteLine("NodeID: " + shardServers.ElementAt(0).nodeID + ", count: " + serverKeys[i].Count);
                        Assert.Fail("Inconsistency at tableID: " + tableID);
                        return;
                    }


                    for (var j = 0; j < serverKeys[0].Count; j++)
                    {
                        var a = serverKeys[i][j];
                        var b = serverKeys[0][j];
                        if (Utils.ByteArraysEqual(a.Key, b.Key) && Utils.ByteArraysEqual(a.Value, b.Value))
                            continue;

                        System.Console.WriteLine("Inconsistency at tableID: " + tableID);
                        System.Console.WriteLine("NodeID: " + shardServers.ElementAt(i).nodeID + ", key: " + Utils.ByteArrayToString(a.Key));
                        System.Console.WriteLine("NodeID: " + shardServers.ElementAt(0).nodeID + ", key: " + Utils.ByteArrayToString(b.Key));
                        Assert.Fail("Inconsistency at tableID: " + tableID);
                        //return;
                        goto Out;
                    }
                }

            Out:
                if (serverKeys[0].Count == 0)
                    break;

                startKey = Utils.NextKey(serverKeys[0].Last().Key);
            }
        }

        public static void FixDiffs(Client client, List<ConfigState.ShardServer> shardServers, Int64 tableID, List<string> diffs)
        {
            var i = 0;
            foreach (var key in diffs)
            {
                i += 1;
                byte[] startKey = Utils.StringToByteArray(key);
                byte[] endKey = Utils.NextKey(startKey);
                var serverKeyValues = ConfigStateHelpers.ParallelFetchTableKeyValuesHTTP(shardServers, tableID, startKey, endKey, true);

                if (Array.TrueForAll(serverKeyValues, val => (val.Count == 1 && Utils.ByteArraysEqual(val.First().Key, serverKeyValues[0].First().Key))))
                    continue;

                foreach (var keyValue in serverKeyValues)
                {
                    if (keyValue == null || keyValue.Count == 0)
                        continue;

                    if (keyValue.First().Value.Length > 0)
                    {
                        Assert.IsTrue(Utils.ByteArraysEqual(Utils.StringToByteArray(key), keyValue.First().Key));
                        
                        Console.WriteLine("Setting key {0}", key);
                        Table table = new Table(client, null, (ulong)tableID, "");
                        table.Set(startKey, keyValue.First().Value);
                        client.Submit();
                    }
                }
            }

        }


        public static void FixConsistency(Client client, List<ConfigState.ShardServer> shardServers, Int64 tableID, string startKey, string endKey)
        {
            if (shardServers.Count <= 1)
                return;

            var keyDiffs = new HashSet<string>();
            var serverKeys = new string[shardServers.Count][];
            var i = 0;

            while (true)
            {
                System.Console.WriteLine("StartKey: " + startKey);
                serverKeys = ConfigStateHelpers.ParallelFetchTableKeysHTTP(shardServers, tableID, startKey, endKey, true);

                for (i = 1; i < serverKeys.Length; i++)
                {
                    if (serverKeys[0].Length > 0 && serverKeys[i].Length > 1 &&
                        serverKeys[0].First().CompareTo(serverKeys[i].Last()) < 0)
                    {
                        foreach (var diff in serverKeys[i].Except(serverKeys[0]))
                            keyDiffs.Add(diff);
                    }

                    if (serverKeys[0].Length > 1 && serverKeys[i].Length > 0 &&
                        serverKeys[i].First().CompareTo(serverKeys[0].Last()) < 0)
                    {
                        foreach (var diff in serverKeys[0].Except(serverKeys[i]))
                            keyDiffs.Add(diff);
                    }
                }

                if (keyDiffs.Count != 0)
                {
                    FixDiffs(client, shardServers, tableID, keyDiffs.ToList());
                    startKey = keyDiffs.Last();
                    keyDiffs = new HashSet<string>();
                    continue;
                }
                
                if (serverKeys[0].Length <= 1)
                    break;

                startKey = serverKeys[0][serverKeys[0].Length - 1];
            }
        }

        public static void CompareNumericTableKeysHTTP(List<ConfigState.ShardServer> shardServers, Int64 tableID, ulong num)
        {
            var serverKeys = new string[shardServers.Count][];
            var startKey = "";
            var endKey = Utils.Id(num);
            ulong counter = 0;
            while (true)
            {
                serverKeys = ConfigStateHelpers.ParallelFetchTableKeysHTTP(shardServers, tableID, startKey, endKey, true);
                var referenceKeys = ConfigStateHelpers.GenerateNumericKeys(counter, (ulong)serverKeys[0].Length);

                for (var i = 0; i < serverKeys.Length; i++)
                {
                    if (!serverKeys[i].SequenceEqual(referenceKeys))
                    {
                        for (var j = 0; j < Math.Max(serverKeys[i].Length, referenceKeys.Length); j++)
                        {
                            var a = serverKeys[i][j];
                            var b = referenceKeys[j];
                            if (a == b)
                                continue;
                            //Assert.IsTrue(a == b);
                            System.Console.WriteLine("Inconsistency at tableID: " + tableID);
                            System.Console.WriteLine("NodeID: " + shardServers.ElementAt(i).nodeID + ", key: " + a);
                            return;
                        }
                    }
                }

                if (serverKeys[0].Length <= 1)
                    break;

                startKey = serverKeys[0][serverKeys[0].Length - 1];
                counter += (ulong)referenceKeys.Length - 1;
                System.Console.WriteLine("StartKey: " + startKey);
            }
        }

        public static void CompareNumericTableKeysBackwardsHTTP(List<ConfigState.ShardServer> shardServers, Int64 tableID, ulong num)
        {
            var serverKeys = new string[shardServers.Count][];
            var startKey = Utils.Id(num);
            var endKey = "";
            ulong counter = num;
            while (true)
            {
                serverKeys = ConfigStateHelpers.ParallelFetchTableKeysHTTP(shardServers, tableID, startKey, endKey, false);
                var referenceKeys = ConfigStateHelpers.GenerateNumericKeysBackwards(counter, (ulong)serverKeys[0].Length);

                for (var i = 0; i < serverKeys.Length; i++)
                {
                    if (!serverKeys[i].SequenceEqual(referenceKeys))
                    {
                        for (var j = 0; j < Math.Max(serverKeys[i].Length, referenceKeys.Length); j++)
                        {
                            var a = serverKeys[i][j];
                            var b = referenceKeys[j];
                            if (a == b)
                                continue;
                            //Assert.IsTrue(a == b);
                            System.Console.WriteLine("Inconsistency at tableID: " + tableID);
                            System.Console.WriteLine("NodeID: " + shardServers.ElementAt(i).nodeID + ", key: " + a);
                            Assert.IsTrue(a == b);
                            return;
                        }
                    }
                }

                if (serverKeys[0].Length <= 1)
                    break;

                startKey = serverKeys[0][serverKeys[0].Length - 1];
                counter -= (ulong)referenceKeys.Length - 1;
                System.Console.WriteLine("StartKey: " + startKey);
            }
        }


        public static void CheckDatabaseWithNumericKeys(string databaseName, string tableName)
        {
            var client = new Client(Utils.GetConfigNodes());
            var jsonConfigState = client.GetJSONConfigState();
            var configState = Utils.JsonDeserialize<ConfigState>(System.Text.Encoding.UTF8.GetBytes(jsonConfigState));
            var shardServers = new List<ConfigState.ShardServer>();
            Int64 tableID = 0;
            foreach (var database in configState.databases)
            {
                if (database.name == databaseName)
                {
                    foreach (var table in configState.tables)
                    {
                        if (table.databaseID == database.databaseID && table.name == tableName)
                        {
                            shardServers = ConfigStateHelpers.GetShardServersByTable(table, configState);
                            tableID = table.tableID;
                            break;
                        }
                    }
                }
                if (tableID != 0)
                    break;
            }

            Assert.IsTrue(tableID != 0);

            ulong num = 50 * 1000 * 1000;
            CompareNumericTableKeysHTTP(shardServers, tableID, num);
            CompareNumericTableKeysBackwardsHTTP(shardServers, tableID, num);
        }

        [TestMethod]
        public void NumericConsistencyTest()
        {
            var dbName = "NumericConsistencyTest";
            var tableName = "test";
            //FillDatabaseWithNumericKeys(dbName, tableName);
            CheckDatabaseWithNumericKeys(dbName, tableName);
        }
    }
}
