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
    public class CounterThreadState
    {
        private static int COUNT_TIMEOUT = 120 * 1000;

        public Thread thread;
        public ConfigState.ShardServer shardServer;
        public string url;
        public Int64 count;
        public TimeSpan elapsed;

        public void CounterThreadFunc()
        {
            DateTime start = DateTime.Now;
            count = GetTableCountHTTP(url);
            elapsed = DateTime.Now.Subtract(start);
        }

        public static Int64 GetTableCountHTTP(string url)
        {
            var result = Utils.HTTP_GET(url, COUNT_TIMEOUT);
            try
            {
                if (result.StartsWith("NEXT"))
                {
                    string[] nextResult = result.Split(new char[] {' '});
                    if (nextResult.Length != 4)
                        return -1;
                    return Convert.ToInt64(nextResult[3].Trim());
                }
                else
                    return Convert.ToInt64(result);
            }
            catch (Exception)
            {
                return -1;
            }
        }
    }

    public class ListerThreadState
    {
        private static int COUNT_TIMEOUT = 120 * 1000;

        public Thread thread;
        public ConfigState.ShardServer shardServer;
        public string url;
        public string[] keys;
        public TimeSpan elapsed;

        public void ListerThreadFunc()
        {
            DateTime start = DateTime.Now;
            keys = GetTableKeysHTTP(url);
            elapsed = DateTime.Now.Subtract(start);
        }

        public static string[] GetTableKeysHTTP(string url)
        {
            var result = Utils.HTTP_GET(url, COUNT_TIMEOUT);
            try
            {
                string[] keys = result.Split(new char[] { '\n' });
                if (keys.Length < 2)
                    return null;
                // last line should be empty line and the one before last should be OK or NEXT
                string lastKey = keys[keys.Length - 1];
                if (lastKey.Length != 0)
                    return null;
                lastKey = keys[keys.Length - 2];
                if (!lastKey.StartsWith("OK") && !lastKey.StartsWith("NEXT"))
                    return null;
                string[] ret = new string[keys.Length - 2];
                Array.Copy(keys, ret, ret.Length);
                return ret;
            }
            catch (Exception)
            {
                return null;
            }
        }
    }


    [TestClass]
    public class ConsistencyTests
    {
        private static int COUNT_TIMEOUT = 120 * 1000;

        [TestMethod]
        public void CheckClusterConsistencyByCount()
        {
            var client = new Client(Config.GetNodes());
            var jsonConfigState = client.GetJSONConfigState();
            var configState = Utils.JsonDeserialize<ConfigState>(System.Text.Encoding.UTF8.GetBytes(jsonConfigState));
            foreach (var table in configState.tables)
            {
                var shards = GetTableShards(table, configState.shards);
                foreach (var shard in shards)
                {
                    System.Console.WriteLine("\nCounting shard " + shard.shardID);

                    var tableID = shard.tableID;
                    var startKey = shard.firstKey;
                    var endKey = shard.lastKey;
                    var quorum = GetQuorum(configState, shard.quorumID);
                    var shardServers = GetQuorumActiveShardServers(configState, quorum);

                    ParallelTableCountHTTP(shardServers, tableID, startKey, endKey);

                    System.Console.WriteLine("Shard " + shard.shardID + " is consistent.\n");
                }
            }
        }

        [TestMethod]
        public void CheckClusterConsistencyByKeys()
        {
            var client = new Client(Config.GetNodes());
            var jsonConfigState = client.GetJSONConfigState();
            var configState = Utils.JsonDeserialize<ConfigState>(System.Text.Encoding.UTF8.GetBytes(jsonConfigState));
            foreach (var table in configState.tables)
            {
                var shards = GetTableShards(table, configState.shards);
                foreach (var shard in shards)
                {
                    System.Console.WriteLine("\nComparing shard keys on shard " + shard.shardID);

                    var tableID = shard.tableID;
                    var startKey = shard.firstKey;
                    var endKey = shard.lastKey;
                    var quorum = GetQuorum(configState, shard.quorumID);
                    var shardServers = GetQuorumActiveShardServers(configState, quorum);

                    CompareTableKeysHTTP(shardServers, tableID, startKey, endKey);
                    
                    System.Console.WriteLine("Shard " + shard.shardID + " is consistent.\n");
                }
            }
        }

        public static string[][] ParallelFetchTableKeysHTTP(List<ConfigState.ShardServer> shardServers, Int64 tableID, string startKey, string endKey, bool forward)
        {
            var threads = new ListerThreadState[shardServers.Count];
            var serverKeys = new string[shardServers.Count][];
            var listGranularity = 100 * 1000 + 1;
            var direction = forward ? "forward" : "backward";

            var i = 0;
            foreach (var shardServer in shardServers)
            {
                var url = GetShardServerURL(shardServer) + "listkeys?tableID=" + tableID + "&startKey=" + startKey + "&endKey=" + endKey + "&count=" + listGranularity + "&direction=" + direction;

                threads[i] = new ListerThreadState();
                threads[i].thread = new Thread(new ThreadStart(threads[i].ListerThreadFunc));
                threads[i].shardServer = shardServer;
                threads[i].url = url;
                threads[i].thread.Start();

                i += 1;
            }

            i = 0;
            foreach (var thread in threads)
            {
                thread.thread.Join();
                serverKeys[i] = thread.keys;

                i += 1;
            }

            return serverKeys;
        }


        public static void CompareTableKeysHTTP(List<ConfigState.ShardServer> shardServers, Int64 tableID, string startKey, string endKey)
        {
            if (shardServers.Count <= 1)
                return;

            var serverKeys = new string[shardServers.Count][];
            var i = 0;
            while (true)
            {
                serverKeys = ParallelFetchTableKeysHTTP(shardServers, tableID, startKey, endKey, true);

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
                            //Assert.IsTrue(a == b);
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
                serverKeys = ParallelFetchTableKeysHTTP(shardServers, tableID, startKey, endKey, true);
                var referenceKeys = GenerateNumericKeys(counter, (ulong) serverKeys[0].Length);

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
                counter += (ulong) referenceKeys.Length - 1;
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
                serverKeys = ParallelFetchTableKeysHTTP(shardServers, tableID, startKey, endKey, false);
                var referenceKeys = GenerateNumericKeysBackwards(counter, (ulong)serverKeys[0].Length);

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


        public static string[] GenerateNumericKeys(ulong start, ulong count)
        {
            var keys = new string[count];

            for (ulong i = 0; i < count; i++)
            {
                var key = start + i;
                keys[i] = Utils.Id(key);
            }

            return keys;
        }

        public static string[] GenerateNumericKeysBackwards(ulong start, ulong count)
        {
            var keys = new string[count];

            for (ulong i = 0; i < count; i++)
            {
                var key = start - i;
                keys[i] = Utils.Id(key);
            }

            return keys;
        }

        public static string[] GetTableKeysHTTP(string url)
        {
            var result = Utils.HTTP_GET(url, COUNT_TIMEOUT);
            try
            {
                string[] keys = result.Split(new char[] {'\n'});
                if (keys.Length < 2)
                    return null;
                // last line should be empty line and the one before last should be OK or NEXT
                string lastKey = keys[keys.Length - 1];
                if (lastKey.Length != 0)
                    return null;
                lastKey = keys[keys.Length - 2];
                if (!lastKey.StartsWith("OK") && !lastKey.StartsWith("NEXT"))
                    return null;
                string[] ret = new string[keys.Length - 2];
                Array.Copy(keys, ret, ret.Length);
                return ret;
            }
            catch (Exception)
            {
                return null;
            }
        }

        public static void ParallelTableCountHTTP(List<ConfigState.ShardServer> shardServers, Int64 tableID, string startKey, string endKey)
        {
            var threads = new CounterThreadState[shardServers.Count];

            var i = 0;
            foreach (var shardServer in shardServers)
            {
                var url = GetShardServerURL(shardServer) + "count?tableID=" + tableID + "&startKey=" + startKey + "&endKey=" + endKey;
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

        public static Int64 GetTableCountHTTP(string url)
        {
            var result = Utils.HTTP_GET(url, COUNT_TIMEOUT);
            try
            {
                return Convert.ToInt64(result);
            }
            catch (Exception e)
            {
                return -1;
            }
        }

        public static ConfigState.Quorum GetQuorum(ConfigState configState, Int64 quorumID)
        {
            foreach (var quorum in configState.quorums)
            {
                if (quorum.quorumID == quorumID)
                    return quorum;
            }
            
            return null;
        }

        public static ConfigState.ShardServer GetShardServer(ConfigState configState, Int64 nodeID)
        {
            foreach (var shardServer in configState.shardServers)
            {
                if (shardServer.nodeID == nodeID)
                    return shardServer;
            }

            return null;
        }

        public static List<ConfigState.ShardServer> GetQuorumActiveShardServers(ConfigState configState, ConfigState.Quorum quorum)
        {
            var shardServers = new List<ConfigState.ShardServer>();
            foreach (var nodeID in quorum.activeNodes)
            {
                var shardServer = GetShardServer(configState, nodeID);
                if (shardServer != null)
                    shardServers.Add(shardServer);
            }

            return shardServers;
        }

        public static List<ConfigState.Shard> GetTableShards(ConfigState.Table table, List<ConfigState.Shard> allShards)
        {
            var shards = new List<ConfigState.Shard>();
            
            foreach (var shard in allShards)
            {
                if (shard.tableID == table.tableID)
                    shards.Add(shard);
            }

            return shards;
        }

        public static List<ConfigState.ShardServer> GetShardServersByTable(ConfigState.Table table, ConfigState configState)
        {
            var shardServers = new List<ConfigState.ShardServer>();

            foreach (var quorum in configState.quorums)
            {
                if (quorum.shards.Intersect(table.shards).Sum() > 0)
                {
                    var quorumServers = GetQuorumActiveShardServers(configState, quorum);
                    foreach (var quorumServer in quorumServers)
                    {
                        if (!shardServers.Contains(quorumServer))
                            shardServers.Add(quorumServer);
                    }
                }
            }

            return shardServers;
        }

        public static string GetEndpointWithPort(string endpoint, uint port)
        {
            int lastIndex = endpoint.LastIndexOf(":");
            if (lastIndex < 0)
                throw new ArgumentException("Bad endpoint: " + endpoint);
            return endpoint.Substring(0, lastIndex + 1) + port;
        }

        public static string GetShardServerURL(ConfigState.ShardServer shardServer)
        {
            return "http://" + GetEndpointWithPort(shardServer.endpoint, shardServer.httpPort) + "/";
        }

        public static bool TryDeleteDatabase(Client client, string databaseName)
        {
            try
            {
                var database = client.GetDatabase(databaseName);
                try
                {
                    database.DeleteDatabase();
                    return true;
                }
                catch (SDBPException)
                {
                    return false;
                }
            }
            catch (SDBPException)
            {
                return true;
            }
        }

        public static Database TryCreateDatabase(Client client, string databaseName)
        {
            try
            {
                var database = client.CreateDatabase(databaseName);
                return database;
            }
            catch (SDBPException)
            {
                return null;
            }
        }

        public static Table TryCreateTable(Database database, string tableName)
        {
            try
            {
                var table = database.CreateTable(tableName);
                return table;
            }
            catch (SDBPException)
            {
                return null;
            }
        }

        public static void FillDatabaseWithNumericKeys(string databaseName, string tableName)
        {
            var client = new Client(Config.GetNodes());
            Assert.IsTrue(TryDeleteDatabase(client, databaseName));
            var database = client.CreateDatabase(databaseName);
            Assert.IsNotNull(database, "Cannot create database " + databaseName);
            var table = TryCreateTable(database, tableName);
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

        public static void CheckDatabaseWithNumericKeys(string databaseName, string tableName)
        {
            var client = new Client(Config.GetNodes());
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
                            shardServers = GetShardServersByTable(table, configState);
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
