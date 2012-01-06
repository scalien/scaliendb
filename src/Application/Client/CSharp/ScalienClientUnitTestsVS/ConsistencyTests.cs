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
                            Assert.IsTrue(a == b);
                            return;
                        }
                    }
                }

                if (serverKeys[0].Length <= 1)
                    break;

                startKey = serverKeys[0][serverKeys[0].Length - 1];
                System.Console.WriteLine("StartKey: " + startKey);
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
