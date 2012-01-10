using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Scalien
{
    public class GetterThreadState
    {
        private static int GET_TIMEOUT = 120 * 1000;

        public ConfigState.ShardServer shardServer;
        public string url;
        public byte[] value;
        public TimeSpan elapsed;
        public ManualResetEvent doneEvent;

        public void GetterThreadFunc(Object threadContext)
        {
            DateTime start = DateTime.Now;
            value = Utils.HTTP.BinaryGET(url, GET_TIMEOUT);
            elapsed = DateTime.Now.Subtract(start);
            doneEvent.Set();
        }
    }

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
            var result = Utils.HTTP.GET(url, COUNT_TIMEOUT);
            try
            {
                if (result.StartsWith("NEXT"))
                {
                    string[] nextResult = result.Split(new char[] { ' ' });
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
            var result = Utils.HTTP.GET(url, COUNT_TIMEOUT);
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

    public class BinaryListerThreadState
    {
        private static int COUNT_TIMEOUT = 120 * 1000;

        public Thread thread;
        public ConfigState.ShardServer shardServer;
        public string url;
        public List<KeyValuePair<byte[], byte[]>> keyValues;
        public TimeSpan elapsed;
        public bool keysOnly;

        public void ListerThreadFunc()
        {
            DateTime start = DateTime.Now;
            keyValues = GetTableKeyValuesHTTP(url);
            elapsed = DateTime.Now.Subtract(start);
        }

        public static int ParseByteArray(byte[] data, int pos, ref byte[] array)
        {
            int prefix = 0;
            while (pos < data.Length)
            {
                if (data[pos] >= '0' && data[pos] <= '9')
                {
                    prefix = prefix * 10 + (data[pos] - '0');
                    pos += 1;
                }
                else if (data[pos] == ':')
                {
                    break;
                }
                else
                    return -1;
            }

            if (pos < data.Length)
            {
                if (data[pos] == ':')
                    pos += 1;
                else
                    return -1;
            }

            if (pos + prefix >= data.Length)
                return -1;

            array = new byte[prefix];
            Buffer.BlockCopy(data, pos, array, 0, prefix);
            pos += prefix;
            return pos;
        }

        public static int ParseKeyValue(byte[] data, int pos, ref KeyValuePair<byte[], byte[]> keyValue)
        {
            byte[] key = null;
            pos = ParseByteArray(data, pos, ref key);
            if (pos == -1)
                return -1;
            if (pos >= data.Length)
                return -1;
            if (data[pos] != ':')
                return -1;
            pos += 1;

            if (data[pos] != ' ')
                return -1;
            pos += 1;

            byte[] value = null;
            pos = ParseByteArray(data, pos, ref value);
            if (pos == -1)
                return -1;
            if (pos >= data.Length)
                return -1;
            if (data[pos] != '\n')
                return -1;
            pos += 1;

            keyValue = new KeyValuePair<byte[], byte[]>(key, value);
            return pos;
        }

        public static List<KeyValuePair<byte[], byte[]>> GetTableKeyValuesHTTP(string url)
        {
            var keyValues = new List<KeyValuePair<byte[], byte[]>>();
            url += "&binary=true";
            var data = Utils.HTTP.BinaryGET(Utils.HTTP.RequestUriString(Utils.StringToByteArray(url)), COUNT_TIMEOUT);
            try
            {
                int pos = 0;
                while (pos >= 0)
                {
                    var keyValue = new KeyValuePair<byte[], byte[]>();
                    pos = ParseKeyValue(data, pos, ref keyValue);
                    if (pos == -1)
                        break;
                    keyValues.Add(keyValue);
                }
            }
            catch (Exception)
            {
                return null;
            }
            return keyValues;
        }
    }

    public class ConfigStateHelpers
    {
        public static string[][] ParallelFetchTableKeysHTTP(List<ConfigState.ShardServer> shardServers, Int64 tableID, string startKey, string endKey, bool forward)
        {
            var threads = new ListerThreadState[shardServers.Count];
            var serverKeys = new string[shardServers.Count][];
            var listGranularity = 100 * 1000;
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

        public static List<KeyValuePair<byte[], byte[]>>[] ParallelFetchTableKeyValuesHTTP(List<ConfigState.ShardServer> shardServers, Int64 tableID, byte[] startKey, byte[] endKey, bool forward)
        {
            var threads = new BinaryListerThreadState[shardServers.Count];
            var serverKeys = new List<KeyValuePair<byte[], byte[]>>[shardServers.Count];
            var listGranularity = 100 * 1000;
            var direction = forward ? "forward" : "backward";

            var i = 0;
            foreach (var shardServer in shardServers)
            {
                var url = Utils.HTTP.BuildUri(GetShardServerURL(shardServer),
                            "listkeyvalues?tableID=" + tableID,
                            "&startKey=", startKey,
                            "&endKey=", endKey,
                            "&count=" + listGranularity,
                            "&direction=" + direction);

                threads[i] = new BinaryListerThreadState();
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
                serverKeys[i] = thread.keyValues;

                i += 1;
            }

            return serverKeys;
        }

        public static List<byte[]> ParallelFetchValuesHTTP(List<ConfigState.ShardServer> shardServers, Int64 tableID, byte[] key)
        {
            var threads = new GetterThreadState[shardServers.Count];
            var doneEvents = new ManualResetEvent[shardServers.Count];
            var serverKeys = new List<byte[]>();

            var i = 0;
            foreach (var shardServer in shardServers)
            {
                var url = Utils.HTTP.BuildUri(GetShardServerURL(shardServer),
                            "raw/get?tableID=" + tableID,
                            "&key=", key);

                doneEvents[i] = new ManualResetEvent(false);

                threads[i] = new GetterThreadState();
                threads[i].shardServer = shardServer;
                threads[i].url = url;
                threads[i].doneEvent = doneEvents[i];
                ThreadPool.QueueUserWorkItem(threads[i].GetterThreadFunc, null);

                i += 1;
            }

            WaitHandle.WaitAll(doneEvents);

            foreach (var thread in threads)
            {
                serverKeys.Add(thread.value);
            }

            return serverKeys;
        }

        public static List<byte[]> ParallelFetchKeyValuesHTTP(List<ConfigState.ShardServer> shardServers, Int64 tableID, byte[] key)
        {
            var threads = new BinaryListerThreadState[shardServers.Count];
            var serverKeys = new List<KeyValuePair<byte[], byte[]>>[shardServers.Count];

            var i = 0;
            foreach (var shardServer in shardServers)
            {
                var url = Utils.HTTP.BuildUri(GetShardServerURL(shardServer),
                            "listkeyvalues?tableID=" + tableID,
                            "&startKey=", key,
                            "&endKey=", key,
                            "&count=" + 1);

                threads[i] = new BinaryListerThreadState();
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
                serverKeys[i] = thread.keyValues;

                i += 1;
            }

            var values = new List<byte[]>();
            for (i = 0; i < serverKeys.Length; i++)
            {
                if (serverKeys[i] != null && serverKeys[i].Count != 0)
                    values.Add(serverKeys[i].First().Value);
                else
                    values.Add(null);
            }
            return values;
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

        public static ConfigState.Table GetTable(ConfigState configState, Int64 tableID)
        {
            foreach (var table in configState.tables)
            {
                if (table.tableID == tableID)
                    return table;
            }

            return null;
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
    }
}
