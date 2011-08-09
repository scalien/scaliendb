using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using System.Linq;
using System.Runtime.InteropServices;

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
    /// <remarks>
    /// TODO.
    /// </remarks>
    /// <example><code>
    /// Client client = new Client({"localhost:7080"});
    /// db = client.GetDatabase("testDatabase");
    /// table = db.GetTable("testTable");
    /// // some sets
    /// using (table.Begin())
    /// {
    ///     for (i = 0; i &lt; 1000; i++) 
    ///         table.Set("foo" + i, "foo" + i);
    /// }
    /// using (table.Begin())
    /// {
    ///     for (i = 0; i &lt; 1000; i++) 
    ///         table.Set("bar" + i, "bar" + i);
    /// }
    /// // some deletes
    /// table.Delete("foo0");
    /// table.Delete("foo10");
    /// // count
    /// System.Console.WriteLine("number of keys starting with foo: " + table.Count(new StringIterParams().Prefix("foo")));
    /// // iterate
    /// foreach(KeyValuePair&lt;string, string&gt; kv in table.KeyValueIterator(new StringIterParams().Prefix("bar")))
    ///     System.Console.WriteLine(kv.Key + " => " + kv.Value);
    /// // truncate
    /// table.Truncate();
    /// </code></example>
    public class Client
    {
        internal SWIGTYPE_p_void cptr;
        private Result result;
        private Result lastResult;

        #region Mode flags

        /// <summary>
        /// Get operations are served by any quorum member. May return stale data.
        /// </summary>
        /// <seealso cref="SetConsistencyLevel(int)"/>
        public const int CONSISTENCY_ANY     = 0;

        /// <summary>
        /// Get operations are served using read-your-writes semantics.
        /// </summary>
        /// <seealso cref="SetConsistencyLevel(int)"/>
        public const int CONSISTENCY_RYW     = 1;

        /// <summary>
        /// Get operations are always served from the primary of each quorum.
        /// This is the default consistency level.
        /// </summary>
        /// <seealso cref="SetConsistencyLevel(int)"/>
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
        /// To construct a Client object, pass in the list of controllers as strings in the form "name:port" or "IP:port".
        /// </summary>
        /// <param name="nodes">The controllers as a list of strings in the form "name:port" or "IP:port".</param>
        /// <example><code>
        /// Client client = new Client({"localhost:7080"});
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
            scaliendb_client.SDBP_Destroy(cptr);
        }

        #endregion

        #region Internal and private helpers

        internal ulong GetQuorumID(string name)
        {
            uint numQuorums = scaliendb_client.SDBP_GetNumQuorums(cptr);
            for (uint i = 0; i < numQuorums; i++)
            {
                ulong quorumID = scaliendb_client.SDBP_GetQuorumIDAt(cptr, i);
                string quorumName = scaliendb_client.SDBP_GetQuorumNameAt(cptr, i);
                if (quorumName == name)
                    return quorumID;
            }
            return 0;
        }

        internal ulong GetDatabaseID()
        {
            return scaliendb_client.SDBP_GetCurrentDatabaseID(cptr);
        }

        internal ulong GetDatabaseID(string name)
        {
            return scaliendb_client.SDBP_GetDatabaseID(cptr, name);
        }
        
        internal ulong GetTableID()
        {
            return scaliendb_client.SDBP_GetCurrentTableID(cptr);
        }

        internal ulong GetTableID(string name)
        {
            return scaliendb_client.SDBP_GetTableID(cptr, GetDatabaseID(), name);
        }

        internal void UseDatabaseID(ulong databaseID)
        {
            int status = scaliendb_client.SDBP_UseDatabaseID(cptr, databaseID);
            CheckStatus(status);
        }

        internal void UseTableID(ulong tableID)
        {
            int status = scaliendb_client.SDBP_UseTableID(cptr, tableID);
            CheckStatus(status);
        }

        internal List<string> ListKeys(string startKey, string endKey, string prefix, uint count, bool skip)
        {
            int status = scaliendb_client.SDBP_ListKeys(cptr, startKey, endKey, prefix, count, skip);
            CheckResultStatus(status);
            return result.GetStringKeys();
        }

        internal List<byte[]> ListKeys(byte[] startKey, byte[] endKey, byte[] prefix, uint count, bool skip)
        {
            int status;

            unsafe
            {
                fixed (byte* pStartKey = startKey, pEndKey = endKey, pPrefix = prefix)
                {
                    IntPtr ipStartKey = new IntPtr(pStartKey);
                    IntPtr ipEndKey = new IntPtr(pEndKey);
                    IntPtr ipPrefix = new IntPtr(pPrefix);
                    status = scaliendb_client.SDBP_ListKeysCStr(cptr, ipStartKey, startKey.Length, ipEndKey, endKey.Length, ipPrefix, prefix.Length, count, skip);
                }
            }
            CheckResultStatus(status);
            return result.GetByteArrayKeys();
        }

        internal Dictionary<string, string> ListKeyValues(string startKey, string endKey, string prefix, uint count, bool skip)
        {
            int status = scaliendb_client.SDBP_ListKeyValues(cptr, startKey, endKey, prefix, count, skip);
            CheckResultStatus(status);
            return result.GetStringKeyValues();
        }

        internal Dictionary<byte[], byte[]> ListKeyValues(byte[] startKey, byte[] endKey, byte[] prefix, uint count, bool skip)
        {
            int status;

            unsafe
            {
                fixed (byte* pStartKey = startKey, pEndKey = endKey, pPrefix = prefix)
                {
                    IntPtr ipStartKey = new IntPtr(pStartKey);
                    IntPtr ipEndKey = new IntPtr(pEndKey);
                    IntPtr ipPrefix = new IntPtr(pPrefix);
                    status = scaliendb_client.SDBP_ListKeyValuesCStr(cptr, ipStartKey, startKey.Length, ipEndKey, endKey.Length, ipPrefix, prefix.Length, count, skip);
                }
            }
            CheckResultStatus(status);
            return result.GetByteArrayKeyValues();
        }

        internal Result GetResult()
        {
            lastResult = result;
            return result;
        }

        private void CheckResultStatus(int status, string msg)
        {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            CheckStatus(status, msg);
        }

        private void CheckResultStatus(int status)
        {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            CheckStatus(status, null);
        }

        private void CheckStatus(int status, string msg)
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

        private void CheckStatus(int status)
        {
            CheckStatus(status, null);
        }

        internal static byte[] StringToByteArray(string str)
        {
            System.Text.UTF8Encoding encoding = new System.Text.UTF8Encoding();
            return encoding.GetBytes(str);
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
        /// The maximum time the client library will wait to complete operations, in miliseconds. Default 120 seconds.
        /// </summary>
        /// <param name="timeout"></param>
        public void SetGlobalTimeout(ulong timeout)
        {
            scaliendb_client.SDBP_SetGlobalTimeout(cptr, timeout);
        }

        /// <summary>
        /// The maximum time the client library will wait to find the master node, in miliseconds. Default 21 seconds.
        /// </summary>
        /// <param name="timeout"></param>
        public void SetMasterTimeout(ulong timeout)
        {
            scaliendb_client.SDBP_SetMasterTimeout(cptr, timeout);
        }

        /// <summary>
        /// Get the global timeout in miliseconds. The global timeout is the maximum time the client library will wait to complete operations.
        /// </summary>
        /// <returns>The global timeout in miliseconds.</returns>
        public ulong GetGlobalTimeout()
        {
            return scaliendb_client.SDBP_GetGlobalTimeout(cptr);
        }

        /// <summary>
        /// Get the master timeout in miliseconds. The master timeout is the maximum time the client library will wait to find the master node.
        /// </summary>
        /// <returns>The master timeout in miliseconds.</returns>
        public ulong GetMasterTimeout()
        {
            return scaliendb_client.SDBP_GetMasterTimeout(cptr);
        }

        /// <summary>
        /// Set the consistency level for Get operations.
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
        /// <param name="consistencyLevel">Client.CONSISTENCY_STRICT or Client.CONSISTENCY_RYW or Client.CONSISTENCY_ANY</param>
        public void SetConsistencyLevel(int consistencyLevel)
        {
            scaliendb_client.SDBP_SetConsistencyLevel(cptr, consistencyLevel);
        }

        /// <summary>
        /// Set the batch mode for write operations (Set and Delete).
        /// </summary>
        /// <remarks>
        /// <para>
        /// As a performance optimization, the ScalienDB can collect write operations in client memory and send them
        /// off in batches to the servers. This will drastically improve the performance of the database.
        /// To send off writes excplicitly, use <see cref="Client.Submit()"/> and <see cref="Table.Submit()"/>.
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
        /// To send off writes excplicitly, use <see cref="Client.Submit()"/> and <see cref="Table.Submit()"/>.
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

        #endregion

        #region Quorum and node management

        /// <summary>
        /// Return a <see cref="Scalien.Quorum"/> by name.
        /// </summary>
        /// <param name="name">The quorum name.</param>
        /// <returns>The <see cref="Quorum"/> object.</returns>
        /// <seealso cref="Quorum"/>
        public Quorum GetQuorum(string name)
        {
            uint numQuorums = scaliendb_client.SDBP_GetNumQuorums(cptr);
            for (uint i = 0; i < numQuorums; i++)
            {
                ulong quorumID = scaliendb_client.SDBP_GetQuorumIDAt(cptr, i);
                string quorumName = scaliendb_client.SDBP_GetQuorumNameAt(cptr, i);
                if (name == quorumName)
                    return new Quorum(this, quorumID, quorumName);
            }
            throw new SDBPException(Status.SDBP_BADSCHEMA, "Quorum not found"); ;
        }

        /// <summary>
        /// Return all quorums in the database as a list of <see cref="Quorum"/> objects.
        /// </summary>
        /// <returns>The list of <see cref="Quorum"/> objects.</returns>
        /// <seealso cref="Quorum"/>
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

        #region Get databases

        /// <summary>
        /// Get a <see cref="Scalien.Database"/> by name
        /// </summary>
        /// <param name="name">The name of the database.</param>
        /// <returns>The <see cref="Database"/> object.</returns>
        /// <seealso cref="Scalien.Database"/>
        public Database GetDatabase(string name)
        {
            return new Database(this, name);
        }

        /// <summary>
        /// Get all databases as a list of <see cref="Database"/> objects
        /// </summary>
        /// <returns>A list of <see cref="Database"/> objects.</returns>
        /// <seealso cref="Scalien.Database"/>
        public List<Database> GetDatabases()
        {
            uint numDatabases = scaliendb_client.SDBP_GetNumDatabases(cptr);
            List<Database> databases = new List<Database>();
            for (uint i = 0; i < numDatabases; i++)
            {
                string name = scaliendb_client.SDBP_GetDatabaseNameAt(cptr, i);
                databases.Add(new Database(this, name));
            }
            return databases;
        }

        #endregion

        #region Database and table management

        /// <summary>
        /// Create a database and return the corresponding <see cref="Database"/> object.
        /// </summary>
        /// <param name="name">The name of the database to create.</param>
        /// <returns>The new <see cref="Database"/> object.</returns>
        /// <seealso cref="Scalien.Database"/>
        public Database CreateDatabase(string name)
        {
            int status = scaliendb_client.SDBP_CreateDatabase(cptr, name);
            CheckResultStatus(status);
            return new Database(this, name);
        }

        /// <summary>
        /// Rename a database.
        /// </summary>
        /// <param name="oldName">The old name.</param>
        /// <param name="newName">the new name.</param>
        public void RenameDatabase(string oldName, string newName)
        {
            int status = scaliendb_client.SDBP_RenameDatabase(cptr, GetDatabaseID(oldName), newName);
            CheckResultStatus(status);
        }

        /// <summary>
        /// Delete a database.
        /// </summary>
        /// <param name="name">The name of the database to delete.</param>
        public void DeleteDatabase(string name)
        {
            int status = scaliendb_client.SDBP_DeleteDatabase(cptr, GetDatabaseID(name));
            CheckResultStatus(status);
        }

        internal Table CreateTable(string tableName)
        {
            List<Quorum> quorums = GetQuorums();
            if (quorums.Count == 0)
                throw new SDBPException(Status.SDBP_BADSCHEMA);
            Quorum quorum = quorums[0];
            int status = scaliendb_client.SDBP_CreateTable(cptr, GetDatabaseID(), quorum.QuorumID, tableName);
            CheckResultStatus(status);
            String databaseName = scaliendb_client.SDBP_GetDatabaseName(cptr, GetDatabaseID());
            return new Table(this, new Database(this, databaseName), tableName);
        }

        internal Table CreateTable(string tableName, string quorumName)
        {
            int status = scaliendb_client.SDBP_CreateTable(cptr, GetDatabaseID(), GetQuorumID(quorumName), tableName);
            CheckResultStatus(status);
            String databaseName = scaliendb_client.SDBP_GetDatabaseName(cptr, GetDatabaseID());
            return new Table(this, new Database(this, databaseName), tableName);
        }

        internal void RenameTable(string oldName, string newName)
        {
            int status = scaliendb_client.SDBP_RenameTable(cptr, GetTableID(oldName), newName);
            CheckResultStatus(status);
        }

        internal void DeleteTable(string name)
        {
            int status = scaliendb_client.SDBP_DeleteTable(cptr, GetTableID(name));
            CheckResultStatus(status);
        }

        internal void TruncateTable(string name)
        {
            int status = scaliendb_client.SDBP_TruncateTable(cptr, GetTableID(name));
            CheckResultStatus(status);
        }

        #endregion

        #region Using... functions

        internal void UseDatabase(string name)
        {
            int status = scaliendb_client.SDBP_UseDatabase(cptr, name);
            CheckStatus(status);
        }
        
        internal void UseTable(string name)
        {
            int status = scaliendb_client.SDBP_UseTable(cptr, name);
            CheckStatus(status);
        }

        #endregion

        #region Data commands

        internal string Get(string key)
        {
            int status = scaliendb_client.SDBP_Get(cptr, key);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return result.GetValue();
        }

        internal byte[] Get(byte[] key)
        {
            int status;
            unsafe
            {
                fixed (byte* pKey = key)
                {
                    IntPtr ipKey = new IntPtr(pKey);
                    status = scaliendb_client.SDBP_GetCStr(cptr, ipKey, key.Length);
                }
            }
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return result.GetValueBytes();
        }

        internal void Set(string key, string value)
        {
            int status = scaliendb_client.SDBP_Set(cptr, key, value);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                if (status == Status.SDBP_BADSCHEMA)
                    CheckStatus(status, "Batch limit exceeded");
                else
                    CheckStatus(status);
            }

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        }

        internal void Set(byte[] key, byte[] value)
        {
            int status;
            unsafe
            {
                fixed (byte* pKey = key, pValue = value)
                {
                    IntPtr ipKey = new IntPtr(pKey);
                    IntPtr ipValue = new IntPtr(pValue);
                    status = scaliendb_client.SDBP_SetCStr(cptr, ipKey, key.Length, ipValue, value.Length);
                }
            }
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                if (status == Status.SDBP_BADSCHEMA)
                    CheckStatus(status, "Batch limit exceeded");
                else
                    CheckStatus(status);
            }

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        }

        internal long Add(string key, long value)
        {
            int status = scaliendb_client.SDBP_Add(cptr, key, value);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return result.GetSignedNumber();
        }

        internal long Add(byte[] key, long value)
        {
            int status;
            unsafe
            {
                fixed (byte* pKey = key)
                {
                    IntPtr ipKey = new IntPtr(pKey);
                    status = scaliendb_client.SDBP_AddCStr(cptr, ipKey, key.Length, value);
                }
            }
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return result.GetSignedNumber();
        }

        internal void Delete(string key)
        {
            int status = scaliendb_client.SDBP_Delete(cptr, key);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                if (status == Status.SDBP_BADSCHEMA)
                    CheckStatus(status, "Batch limit exceeded");
                else
                    CheckStatus(status);
            }

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        }

        internal void Delete(byte[] key)
        {
            int status;
            unsafe
            {
                fixed (byte* pKey = key)
                {
                    IntPtr ipKey = new IntPtr(pKey);
                    status = scaliendb_client.SDBP_DeleteCStr(cptr, ipKey, key.Length);
                }
            }
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                if (status == Status.SDBP_BADSCHEMA)
                    CheckStatus(status, "Batch limit exceeded");
                else
                    CheckStatus(status);
            }

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        }

        internal ulong Count(StringIterParams ps)
        {
            int status = scaliendb_client.SDBP_Count(cptr, ps.startKey, ps.endKey, ps.prefix);
            CheckResultStatus(status);
            return result.GetNumber();
        }

        internal ulong Count(ByteIterParams ps)
        {
            int status;

            unsafe
            {
                fixed (byte* pStartKey = ps.startKey, pEndKey = ps.endKey, pPrefix = ps.prefix)
                {
                    IntPtr ipStartKey = new IntPtr(pStartKey);
                    IntPtr ipEndKey = new IntPtr(pEndKey);
                    IntPtr ipPrefix = new IntPtr(pPrefix);
                    status = scaliendb_client.SDBP_CountCStr(cptr, ipStartKey, ps.startKey.Length, ipEndKey, ps.endKey.Length, ipPrefix, ps.prefix.Length);
                }
            }
            CheckResultStatus(status);
            return result.GetNumber();
        }

        #endregion

        #region Submit, Cancel

        internal SubmitGuard Begin()
        {
            return new SubmitGuard(this);
        }

        internal void Submit()
        {
            int status = scaliendb_client.SDBP_Submit(cptr);
            CheckStatus(status);
        }

        internal void Cancel()
        {
            int status = scaliendb_client.SDBP_Cancel(cptr);
            CheckStatus(status);
        }

        #endregion
    }
}
