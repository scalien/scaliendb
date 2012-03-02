using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;

namespace Scalien
{
    /// <summary>
    /// ScalienDB exceptions.
    /// </summary>
    public class SDBPException : Exception
    {
        private int status;

        /// <summary>
        /// The status code of the exception.
        /// </summary>
        public int Status
        {
            get
            {
                return status;
            }
        }

        internal SDBPException(int status)
            : base(Scalien.Status.ToString(status) + " (code " + status + ")")
        {
            this.status = status;
        }

        internal SDBPException(int status, string txt)
            : base(Scalien.Status.ToString(status) + " (code " + status + "): " + txt)
        {
            this.status = status;
        }
    }

    /// <summary>
    /// The ScalienDB client represents a connection to a ScalienDB cluster.
    /// </summary>
    /// <example><code>
    /// string[] controllers = { "localhost:7080" };
    /// Client client = new Client(controllers);
    /// db = client.GetDatabase("testDatabase");
    /// table = db.GetTable("testTable");
    /// // some sets
    /// using (client.Begin())
    /// {
    ///     for (i = 0; i &lt; 1000; i++) 
    ///         table.Set("foo" + i, "foo" + i);
    /// }
    /// using (client.Begin())
    /// {
    ///     for (i = 0; i &lt; 1000; i++) 
    ///         table.Set("bar" + i, "bar" + i);
    /// }
    /// // some deletes
    /// table.Delete("foo0");
    /// table.Delete("foo10");
    /// client.Submit();
    /// // count
    /// System.Console.WriteLine("number of keys starting with foo: " + table.Count(new StringRangeParams().Prefix("foo")));
    /// // iterate
    /// foreach(KeyValuePair&lt;string, string&gt; kv in table.GetKeyValueIterator(new StringRangeParams().Prefix("bar")))
    ///     System.Console.WriteLine(kv.Key + " => " + kv.Value);
    /// // truncate
    /// table.Truncate();
    /// </code></example>
    public class Client
    {
        internal SWIGTYPE_p_void cptr;
        private Result result;

        #region Mode flags

        /// <summary>
        /// Get operations are served by any quorum member. May return stale data.
        /// </summary>
        /// <seealso cref="SetConsistencyMode(int)"/>
        public const int CONSISTENCY_ANY     = 0;

        /// <summary>
        /// Get operations are served using read-your-writes semantics.
        /// </summary>
        /// <seealso cref="SetConsistencyMode(int)"/>
        public const int CONSISTENCY_RYW     = 1;

        /// <summary>
        /// Get operations are always served from the primary of each quorum.
        /// This is the default consistency level.
        /// </summary>
        /// <seealso cref="SetConsistencyMode(int)"/>
        public const int CONSISTENCY_STRICT = 2;

        /// <summary>
        /// Default batch mode. Writes are batched, then sent off to the ScalienDB server.
        /// </summary>
        /// <seealso cref="SetBatchMode(int)"/>
        /// <seealso cref="SetBatchLimit(uint)"/>
        public const int BATCH_DEFAULT       = 0;

        /// <summary>
        /// Writes are batched, and if the batch limit is reached an exception is thrown.
        /// </summary>
        /// <seealso cref="SDBPException"/>
        /// <seealso cref="SetBatchMode(int)"/>
        /// <seealso cref="SetBatchLimit(uint)"/>
        public const int BATCH_NOAUTOSUBMIT  = 1;

        /// <summary>
        /// Writes are sent one by one. This will be much slower than the default mode.
        /// </summary>
        /// <seealso cref="SetBatchMode(int)"/>
        /// <seealso cref="SetBatchLimit(uint)"/>
        public const int BATCH_SINGLE        = 2;

        #endregion

        #region Constructors, destructors

        /// <summary>
        /// Construct a Client object. Pass in the list of controllers as strings in the form "host:port".
        /// </summary>
        /// <param name="nodes">The controllers as a list of strings in the form "host:port".</param>
        /// <example><code>
        /// Client client = new Client({"192.168.1.1:7080", "192.168.1.2:7080"});
        /// </code></example>
        public Client(string[] nodes)
        {
            cptr = scaliendb_client.SDBP_Create();
            result = null;

            SDBP_NodeParams nodeParams = new SDBP_NodeParams(nodes.Length);
            for (int i = 0; i < nodes.Length; i++)
                nodeParams.AddNode(nodes[i]);

            int status = scaliendb_client.SDBP_Init(cptr, nodeParams);
            nodeParams.Close();
        }

        /// <summary>
        /// </summary>
        ~Client()
        {
            Close();
        }

        /// <summary>
        /// Close the client object.
        /// </summary>
        public void Close()
        {
            var oldPtr = Interlocked.Exchange(ref cptr, null);
            if (oldPtr != null)
            {
                scaliendb_client.SDBP_Destroy(oldPtr);
            }
        }

        #endregion

        #region Internal and private helpers

        internal List<string> ListKeys(ulong tableID, string startKey, string endKey, string prefix, uint count, bool forwardDirection, bool skip)
        {
            List<byte[]> byteKeys = ListKeys(tableID, StringToByteArray(startKey), StringToByteArray(endKey), StringToByteArray(prefix), count, forwardDirection, skip);
            List<string> stringKeys = new List<string>();
            foreach (byte[] byteKey in byteKeys)
                stringKeys.Add(ByteArrayToString(byteKey));
            return stringKeys;
        }

        internal List<byte[]> ListKeys(ulong tableID, byte[] startKey, byte[] endKey, byte[] prefix, uint count, bool forwardDirection, bool skip)
        {
            int status;

            unsafe
            {
                fixed (byte* pStartKey = startKey, pEndKey = endKey, pPrefix = prefix)
                {
                    IntPtr ipStartKey = new IntPtr(pStartKey);
                    IntPtr ipEndKey = new IntPtr(pEndKey);
                    IntPtr ipPrefix = new IntPtr(pPrefix);
                    status = scaliendb_client.SDBP_ListKeysCStr(cptr, tableID,  ipStartKey, startKey.Length, ipEndKey, endKey.Length, ipPrefix, prefix.Length, count, forwardDirection, skip);
                }
            }
            CheckResultStatus(status);
            List<byte[]> keys = result.GetByteArrayKeys();
            result.Close();
            return keys;
        }

        internal Dictionary<string, string> ListKeyValues(ulong tableID, string startKey, string endKey, string prefix, uint count, bool forwardDirection, bool skip)
        {
            Dictionary<byte[], byte[]> byteKeyValues = ListKeyValues(tableID, StringToByteArray(startKey), StringToByteArray(endKey), StringToByteArray(prefix), count, forwardDirection, skip);
            Dictionary<string, string> stringKeyValues = new Dictionary<string, string>();
            foreach (KeyValuePair<byte[], byte[]> kv in byteKeyValues)
                stringKeyValues.Add(ByteArrayToString(kv.Key), ByteArrayToString(kv.Value));
            return stringKeyValues;
        }

        internal Dictionary<byte[], byte[]> ListKeyValues(ulong tableID, byte[] startKey, byte[] endKey, byte[] prefix, uint count, bool forwardDirection, bool skip)
        {
            int status;

            unsafe
            {
                fixed (byte* pStartKey = startKey, pEndKey = endKey, pPrefix = prefix)
                {
                    IntPtr ipStartKey = new IntPtr(pStartKey);
                    IntPtr ipEndKey = new IntPtr(pEndKey);
                    IntPtr ipPrefix = new IntPtr(pPrefix);
                    status = scaliendb_client.SDBP_ListKeyValuesCStr(cptr, tableID, ipStartKey, startKey.Length, ipEndKey, endKey.Length, ipPrefix, prefix.Length, count, forwardDirection, skip);
                }
            }
            CheckResultStatus(status);
            Dictionary<byte[], byte[]> keyValues = result.GetByteArrayKeyValues();
            result.Close();
            return keyValues;
        }

        internal void CheckResultStatus(int status, string msg)
        {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            CheckStatus(status, msg);
        }

        internal void CheckResultStatus(int status)
        {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            CheckStatus(status, null);
        }

        internal void CheckStatus(int status, string msg)
        {
            if (status < 0)
            {
                if (msg != null)
                    throw new SDBPException(status, msg);
                if (status == Status.SDBP_BADSCHEMA)
                    throw new SDBPException(status, "No database or table is in use");
                if (status == Status.SDBP_NOSERVICE)
                    throw new SDBPException(status, "No server in the cluster was able to serve the request");
                throw new SDBPException(status);
            }
        }

        internal void CheckStatus(int status)
        {
            CheckStatus(status, null);
        }

        internal static byte[] StringToByteArray(string str)
        {
            return System.Text.Encoding.UTF8.GetBytes(str);
        }

        internal static string ByteArrayToString(byte[] data)
        {
            if (data == null)
                return null;
            return System.Text.UTF8Encoding.UTF8.GetString(data);
        }

        internal class ByteArrayComparer : IEqualityComparer<byte[]>
        {
            public bool Equals(byte[] b1, byte[] b2)
            {
                return ByteArrayCompare(b1, b2);
            }

            public int GetHashCode(byte[] key)
            {
                if (key == null)
                    throw new ArgumentNullException("key");
                return key.Sum(i => i);
            }
        }

        [DllImport("msvcrt.dll")]
        internal static extern int memcmp(byte[] b1, byte[] b2, long count);

        internal static bool ByteArrayCompare(byte[] b1, byte[] b2)
        {
            // Validate buffers are the same length.
            // This also ensures that the count does not exceed the length of either buffer.  
            return b1.Length == b2.Length && memcmp(b1, b2, b1.Length) == 0;
        }

        #endregion

        #region Timeouts, modes

        /// <summary>
        /// Turn on additional log trace output in the underlying C++ client library. Off by default.
        /// </summary>
        /// <param name="trace">True or false.</param>
        public static void SetTrace(bool trace)
        {
            scaliendb_client.SDBP_SetTrace(trace);
        }

        /// <summary>
        /// Turn on crash reporter functionality in in the underlying C++ client library. Off by default.
        /// </summary>
        /// <param name="crashReporter">True or false.</param>
        public static void SetCrashReporter(bool crashReporter)
        {
            scaliendb_client.SDBP_SetCrashReporter(crashReporter);
        }

        /// <summary>
        /// Sets the name of the trace log output file in the underlying C++ client library.
        /// </summary>
        /// <param name="filename">The name of the logfile.</param>
        public static void SetLogFile(string filename)
        {
            scaliendb_client.SDBP_SetLogFile(filename);
        }

        /// <summary>
        /// Send a log message through the underlying C++ client library.
        /// </summary>
        /// <param name="msg">The message.</param>
        public static void LogTrace(string msg)
        {
            scaliendb_client.SDBP_LogTrace(msg);
        }

        /// <summary>
        /// Set the size of the connection pool.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Sets the number of connections that are kept in the pool. Setting this value to 0 means no 
        /// connection pooling.
        /// </para>
        /// </remarks>
        /// <param name="poolSize">The size of the pool</param>
        public static void SetConnectionPoolSize(uint poolSize)
        {
            scaliendb_client.SDBP_SetShardPoolSize(poolSize);
        }

        /// <summary>
        /// Set the maximum number of connections.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Sets the maximum number of network connections to be made from the client.
        /// </para>
        /// </remarks>
        /// <param name="maxConnections">The maximum number of connections</param>
        public static void SetMaxConnections(uint maxConnections)
        {
            scaliendb_client.SDBP_SetMaxConnections(maxConnections);
        }

        /// <summary>
        /// The maximum time the client library will wait to complete operations, in miliseconds. Default 120 seconds.
        /// </summary>
        /// <param name="timeout">The global timeout in miliseconds.</param>
        /// <seealso cref="SetMasterTimeout(ulong)"/>
        /// <seealso cref="GetGlobalTimeout()"/>
        /// <seealso cref="GetMasterTimeout()"/>
        public void SetGlobalTimeout(ulong timeout)
        {
            scaliendb_client.SDBP_SetGlobalTimeout(cptr, timeout);
        }

        /// <summary>
        /// The maximum time the client library will wait to find the master node, in miliseconds. Default 21 seconds.
        /// </summary>
        /// <param name="timeout">The master timeout in miliseconds. </param>
        /// <seealso cref="SetGlobalTimeout(ulong)"/>
        /// <seealso cref="GetGlobalTimeout()"/>
        /// <seealso cref="GetMasterTimeout()"/>
        public void SetMasterTimeout(ulong timeout)
        {
            scaliendb_client.SDBP_SetMasterTimeout(cptr, timeout);
        }

        /// <summary>
        /// Get the global timeout in miliseconds. The global timeout is the maximum time the client library will wait to complete operations.
        /// </summary>
        /// <returns>The global timeout in miliseconds.</returns>
        /// <seealso cref="GetMasterTimeout()"/>
        /// <seealso cref="SetGlobalTimeout(ulong)"/>
        /// <seealso cref="SetMasterTimeout(ulong)"/>
        public ulong GetGlobalTimeout()
        {
            return scaliendb_client.SDBP_GetGlobalTimeout(cptr);
        }

        /// <summary>
        /// Get the master timeout in miliseconds. The master timeout is the maximum time the client library will wait to find the master node.
        /// </summary>
        /// <returns>The master timeout in miliseconds.</returns>
        /// <seealso cref="GetGlobalTimeout()"/>
        /// <seealso cref="SetGlobalTimeout(ulong)"/>
        /// <seealso cref="SetMasterTimeout(ulong)"/>
        public ulong GetMasterTimeout()
        {
            return scaliendb_client.SDBP_GetMasterTimeout(cptr);
        }

        /// <summary>
        /// Set the consistency mode for Get operations.
        /// </summary>
        /// <remarks>
        /// Possible values are:
        /// <list type="bullet">
        /// <item>Client.CONSISTENCY_STRICT: Get operations are sent to the primary of the quorum.</item>
        /// <item>Client.CONSISTENCY_RYW: RYW is short for Read Your Writers. Get operations may be
        /// sent to slaves, but the internal replication number is used as a sequencer to make sure
        /// the slave has seen the last write issued by the client.</item>
        /// <item>Client.CONSISTENCY_ANY: Get operations may be sent to slaves, which may return stale data.</item>
        /// </list>
        /// The default is Client.CONSISTENCY_STRICT.
        /// </remarks>
        /// <param name="consistencyMode">Client.CONSISTENCY_STRICT or Client.CONSISTENCY_RYW or Client.CONSISTENCY_ANY</param>
        public void SetConsistencyMode(int consistencyMode)
        {
            scaliendb_client.SDBP_SetConsistencyMode(cptr, consistencyMode);
        }

        /// <summary>
        /// Set the batch mode for write operations (Set and Delete).
        /// </summary>
        /// <remarks>
        /// <para>
        /// As a performance optimization, the ScalienDB can collect write operations in client memory and send them
        /// off in batches to the servers. This will drastically improve the performance of the database.
        /// To send off writes excplicitly, use <see cref="Client.Submit()"/>.
        /// </para>
        /// <para>
        /// The modes are:
        /// <list type="bullet">
        /// <item>Client.BATCH_DEFAULT: collect writes until the batch limit is reached, then send them off to the servers</item>
        /// <item>Client.BATCH_NOAUTOSUBMIT: collect writes until the batch limit is reached, then throw an <see cref="SDBPException"/></item>
        /// <item>Client.BATCH_SINGLE: send writes one by one, as they are issued</item>
        /// </list>
        /// The default is Client.BATCH_DEFAULT.
        /// </para>
        /// </remarks>
        /// <param name="batchMode">Client.BATCH_DEFAULT or Client.BATCH_NOAUTOSUBMIT or Client.BATCH_SINGLE</param>
        /// <seealso cref="SetBatchLimit(uint)"/>
        public void SetBatchMode(int batchMode)
        {
            scaliendb_client.SDBP_SetBatchMode(cptr, batchMode);
        }

        /// <summary>
        /// The the batch limit for write operations (Set and Delete) in bytes.
        /// </summary>
        /// <remarks>
        /// <para>
        /// When running with batchMode = Client.BATCH_DEFAULT and Client.BATCH_NOAUTOSUBMIT, the client collects writes
        /// and sends them off to the ScalienDB server in batches.
        /// To send off writes excplicitly, use <see cref="Client.Submit()"/>.
        /// </para>
        /// <para>SetBatchLimit() lets you specify the exact amount to store in memory before:
        /// <list type="bullet">
        /// <item>submiting the operations in Client.BATCH_DEFAULT mode</item>
        /// <item>throwing an <see cref="Scalien.SDBPException"/> in Client.BATCH_NOAUTOSUBMIT mode</item>
        /// </list>
        /// </para>
        /// <para>
        /// The default 1MB.
        /// </para>
        /// </remarks>
        /// <param name="batchLimit">The batch limit in bytes.</param>
        /// <seealso cref="SetBatchMode(int)"/>
        public void SetBatchLimit(uint batchLimit)
        {
            scaliendb_client.SDBP_SetBatchLimit(cptr, batchLimit);
        }

        /// <summary>
        /// Return the config state as a JSON string.
        /// </summary>
        public string GetJSONConfigState()
        {
            return scaliendb_client.SDBP_GetJSONConfigState(cptr);
        }

        #endregion

        #region Quorum and node management

        /// <summary>
        /// Return a <see cref="Scalien.Quorum"/> by name.
        /// </summary>
        /// <param name="name">The quorum name.</param>
        /// <returns>The <see cref="Quorum"/> object.</returns>
        /// <seealso cref="Quorum"/>
        /// <seealso cref="GetQuorums()"/>
        public Quorum GetQuorum(string name)
        {
            List<Quorum> quorums = GetQuorums();
            foreach (Quorum quorum in quorums)
            {
                if (quorum.Name == name)
                    return quorum;
            }
            throw new SDBPException(Status.SDBP_BADSCHEMA, "Quorum not found");
        }

        /// <summary>
        /// Return all quorums in the database as a list of <see cref="Quorum"/> objects.
        /// </summary>
        /// <returns>The list of <see cref="Quorum"/> objects.</returns>
        /// <seealso cref="Quorum"/>
        /// <seealso cref="GetQuorum(string)"/>
        public List<Quorum> GetQuorums()
        {
            uint numQuorums = scaliendb_client.SDBP_GetNumQuorums(cptr);
            List<Quorum> quorums = new List<Quorum>();
            for (uint i = 0; i < numQuorums; i++)
            {
                ulong quorumID = scaliendb_client.SDBP_GetQuorumIDAt(cptr, i);
                string quorumName = scaliendb_client.SDBP_GetQuorumNameAt(cptr, i);
                quorums.Add(new Quorum(this, quorumID, quorumName));
            }
            return quorums;
        }

        #endregion

        #region Database management

        /// <summary>
        /// Get a <see cref="Scalien.Database"/> by name.
        /// </summary>
        /// <param name="name">The name of the database.</param>
        /// <returns>The <see cref="Database"/> object or <code>null</code>.</returns>
        /// <seealso cref="Scalien.Database"/>
        /// <seealso cref="GetDatabases()"/>
        /// <seealso cref="CreateDatabase(string)"/>
        public Database GetDatabase(string name)
        {
            uint numDatabases = scaliendb_client.SDBP_GetNumDatabases(cptr);
            for (uint i = 0; i < numDatabases; i++)
            {
                ulong databaseID = scaliendb_client.SDBP_GetDatabaseIDAt(cptr, i);
                string databaseName = scaliendb_client.SDBP_GetDatabaseNameAt(cptr, i);
                if (name == databaseName)
                    return new Database(this, databaseID, name);
            }
            throw new SDBPException(Status.SDBP_BADSCHEMA, "Database not found");
        }

        /// <summary>
        /// Get all databases as a list of <see cref="Database"/> objects.
        /// </summary>
        /// <returns>A list of <see cref="Database"/> objects.</returns>
        /// <seealso cref="Scalien.Database"/>
        /// <seealso cref="GetDatabase(string)"/>
        /// <seealso cref="CreateDatabase(string)"/>
        public List<Database> GetDatabases()
        {
            uint numDatabases = scaliendb_client.SDBP_GetNumDatabases(cptr);
            List<Database> databases = new List<Database>();
            for (uint i = 0; i < numDatabases; i++)
            {
                ulong databaseID = scaliendb_client.SDBP_GetDatabaseIDAt(cptr, i);
                string name = scaliendb_client.SDBP_GetDatabaseNameAt(cptr, i);
                databases.Add(new Database(this, databaseID, name));
            }
            return databases;
        }

        /// <summary>
        /// Create a database and return the corresponding <see cref="Database"/> object.
        /// </summary>
        /// <param name="name">The name of the database to create.</param>
        /// <returns>The new <see cref="Database"/> object.</returns>
        /// <seealso cref="Scalien.Database"/>
        /// <seealso cref="GetDatabase(string)"/>
        /// <seealso cref="GetDatabases()"/>
        public Database CreateDatabase(string name)
        {
            int status = scaliendb_client.SDBP_CreateDatabase(cptr, name);
            CheckResultStatus(status);
            return GetDatabase(name);
        }

        #endregion

        #region Data commands

        internal string Get(ulong tableID, string key)
        {
            return ByteArrayToString(Get(tableID, StringToByteArray(key)));
        }

        internal byte[] Get(ulong tableID, byte[] key)
        {
            int status;
            unsafe
            {
                fixed (byte* pKey = key)
                {
                    IntPtr ipKey = new IntPtr(pKey);
                    status = scaliendb_client.SDBP_GetCStr(cptr, tableID, ipKey, key.Length);
                }
            }
            if (status < 0)
            {
                if (status == Status.SDBP_FAILED)
                    return null;
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
                return result.GetValueBytes();
            }

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            byte[] value = result.GetValueBytes();
            result.Close();
            return value;
        }

        internal void Set(ulong tableID, string key, string value)
        {
            Set(tableID, StringToByteArray(key), StringToByteArray(value));
        }

        internal void Set(ulong tableID, byte[] key, byte[] value)
        {
            int status;
            unsafe
            {
                fixed (byte* pKey = key, pValue = value)
                {
                    IntPtr ipKey = new IntPtr(pKey);
                    IntPtr ipValue = new IntPtr(pValue);
                    status = scaliendb_client.SDBP_SetCStr(cptr, tableID, ipKey, key.Length, ipValue, value.Length);
                }
            }
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            if (status < 0)
            {
                if (status == Status.SDBP_BADSCHEMA)
                    CheckStatus(status, "Batch limit exceeded");
                else
                    CheckStatus(status);
            }
            result.Close();
        }

        internal long Add(ulong tableID, string key, long value)
        {
            return Add(tableID, StringToByteArray(key), value);
        }

        internal long Add(ulong tableID, byte[] key, long value)
        {
            int status;
            unsafe
            {
                fixed (byte* pKey = key)
                {
                    IntPtr ipKey = new IntPtr(pKey);
                    status = scaliendb_client.SDBP_AddCStr(cptr, tableID, ipKey, key.Length, value);
                }
            }
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            if (status < 0)
            {
                CheckStatus(status);
            }

            long number = result.GetSignedNumber();
            result.Close();
            return number;
        }

        internal void Delete(ulong tableID, string key)
        {
            Delete(tableID, StringToByteArray(key));
        }

        internal void Delete(ulong tableID, byte[] key)
        {
            int status;
            unsafe
            {
                fixed (byte* pKey = key)
                {
                    IntPtr ipKey = new IntPtr(pKey);
                    status = scaliendb_client.SDBP_DeleteCStr(cptr, tableID, ipKey, key.Length);
                }
            }
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            if (status < 0)
            {
                if (status == Status.SDBP_BADSCHEMA)
                    CheckStatus(status, "Batch limit exceeded");
                else
                    CheckStatus(status);
            }
            result.Close();
        }

        internal void SequenceSet(ulong tableID, string key, ulong value)
        {
            SequenceSet(tableID, StringToByteArray(key), value);
        }

        internal void SequenceSet(ulong tableID, byte[] key, ulong value)
        {
            int status;
            unsafe
            {
                fixed (byte* pKey = key)
                {
                    IntPtr ipKey = new IntPtr(pKey);
                    status = scaliendb_client.SDBP_SequenceSetCStr(cptr, tableID, ipKey, key.Length, value);
                }
            }
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            if (status < 0)
            {
                CheckStatus(status);
            }

            result.Close();
        }

        internal ulong SequenceNext(ulong tableID, string key)
        {
            return SequenceNext(tableID, StringToByteArray(key));
        }

        internal ulong SequenceNext(ulong tableID, byte[] key)
        {
            int status;
            unsafe
            {
                fixed (byte* pKey = key)
                {
                    IntPtr ipKey = new IntPtr(pKey);
                    status = scaliendb_client.SDBP_SequenceNextCStr(cptr, tableID, ipKey, key.Length);
                }
            }
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            if (status < 0)
            {
                CheckStatus(status);
            }

            ulong number = result.GetNumber();
            result.Close();
            return number;
        }

        internal ulong Count(ulong tableID, StringRangeParams ps)
        {
            return Count(tableID, ps.ToByteRangeParams());
        }

        internal ulong Count(ulong tableID, ByteRangeParams ps)
        {
            int status;

            unsafe
            {
                fixed (byte* pStartKey = ps.startKey, pEndKey = ps.endKey, pPrefix = ps.prefix)
                {
                    IntPtr ipStartKey = new IntPtr(pStartKey);
                    IntPtr ipEndKey = new IntPtr(pEndKey);
                    IntPtr ipPrefix = new IntPtr(pPrefix);
                    status = scaliendb_client.SDBP_CountCStr(cptr, tableID, ipStartKey, ps.startKey.Length, ipEndKey, ps.endKey.Length, ipPrefix, ps.prefix.Length, ps.forwardDirection);
                }
            }
            CheckResultStatus(status);
            ulong number = result.GetNumber();
            result.Close();
            return number;
        }

        #endregion

        #region Submit, Cancel

        /// <summary>
        /// Begin a client-side transaction.
        /// </summary>
        /// <remarks>
        /// Return a <see cref="Scalien.Submitter"/> object that will automatically call <see cref="Submit()"/> when it goes out of scope.
        /// </remarks>
        /// <returns>A <see cref="Scalien.Submitter"/> object that will automatically call <see cref="Submit()"/> when it goes out of scope.</returns>
        /// <seealso cref="Submit()"/>
        /// <seealso cref="Rollback()"/>
        public Submitter Begin()
        {
            return new Submitter(this);
        }

        /// <summary>
        /// Send the batched operations to the ScalienDB server.
        /// </summary>
        /// <seealso cref="Begin()"/>
        /// <seealso cref="Rollback()"/>
        public void Submit()
        {
            int status = scaliendb_client.SDBP_Submit(cptr);
            CheckStatus(status);
        }

        /// <summary>
        /// Discard all batched operations.
        /// </summary>
        /// <seealso cref="Begin()"/>
        /// <seealso cref="Submit()"/>
        public void Rollback()
        {
            int status = scaliendb_client.SDBP_Cancel(cptr);
            CheckStatus(status);
        }

        #endregion
    }
}
