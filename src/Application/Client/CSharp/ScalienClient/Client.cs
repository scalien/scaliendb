using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using System.Linq;
using System.Runtime.InteropServices;

namespace Scalien
{
    public class SDBPException : Exception
    {
        private int status;
        public int Status
        {
            get
            {
                return status;
            }
        }

        public SDBPException(int status)
            : base(Scalien.Status.ToString(status) + " (code " + status + ")")
        {
            this.status = status;
        }

        public SDBPException(int status, string txt)
            : base(Scalien.Status.ToString(status) + " (code " + status + "): " + txt)
        {
            this.status = status;
        }
    }

    public class Client
    {
        internal SWIGTYPE_p_void cptr;
        private Result result;
        private Result lastResult;

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

        ~Client()
        {
            scaliendb_client.SDBP_Destroy(cptr);
        }

        public void SetGlobalTimeout(ulong timeout)
        {
            scaliendb_client.SDBP_SetGlobalTimeout(cptr, timeout);
        }

        public void SetMasterTimeout(ulong timeout)
        {
            scaliendb_client.SDBP_SetMasterTimeout(cptr, timeout);
        }

        public ulong GetGlobalTimeout()
        {
            return scaliendb_client.SDBP_GetGlobalTimeout(cptr);
        }

        public ulong GetMasterTimeout()
        {
            return scaliendb_client.SDBP_GetMasterTimeout(cptr);
        }

        public void SetConsistencyLevel(int consistencyLevel)
        {
            scaliendb_client.SDBP_SetConsistencyLevel(cptr, consistencyLevel);
        }

        public void SetBatchLimit(uint batchLimit)
        {
            scaliendb_client.SDBP_SetBatchLimit(cptr, batchLimit);
        }

        public string GetJSONConfigState()
        {
            return scaliendb_client.SDBP_GetJSONConfigState(cptr);
        }

        public Quorum GetQuorum(ulong quorumID)
        {
            return new Quorum(this, quorumID);
        }

        public List<Quorum> GetQuorums()
        {
            uint numQuorums = scaliendb_client.SDBP_GetNumQuorums(cptr);
            List<Quorum> quorums = new List<Quorum>();
            for (uint i = 0; i < numQuorums; i++)
            {
                ulong quorumID = scaliendb_client.SDBP_GetQuorumIDAt(cptr, i);
                quorums.Add(new Quorum(this, quorumID));
            }
            return quorums;
        }

        public Quorum CreateQuorum(string name, ulong[] nodes)
        {
            SDBP_NodeParams nodeParams = new SDBP_NodeParams(nodes.Length);
            for (int i = 0; i < nodes.Length; i++)
            {
                string nodeString = "" + nodes[i];
                nodeParams.AddNode(nodeString);
            }

            int status = scaliendb_client.SDBP_CreateQuorum(cptr, name, nodeParams);
            nodeParams.Close();

            CheckResultStatus(status, "Cannot create quorum");
            return new Quorum(this, result.GetNumber());
        }

        public void DeleteQuorum(ulong quorumID)
        {
            int status = scaliendb_client.SDBP_DeleteQuorum(cptr, quorumID);
            CheckResultStatus(status, "Cannot delete quorum");
        }

        public void AddNode(ulong quorumID, ulong nodeID)
        {
            int status = scaliendb_client.SDBP_AddNode(cptr, quorumID, nodeID);
            CheckResultStatus(status, "Cannot add node"); 
        }

        public void RemoveNode(ulong quorumID, ulong nodeID)
        {
            int status = scaliendb_client.SDBP_RemoveNode(cptr, quorumID, nodeID);
            CheckResultStatus(status, "Cannot remove node");
        }

        public void ActivateNode(ulong nodeID)
        {
            int status = scaliendb_client.SDBP_ActivateNode(cptr, nodeID);
            CheckResultStatus(status, "Cannot activate node");
        }

        public Database CreateDatabase(string name)
        {
            int status = scaliendb_client.SDBP_CreateDatabase(cptr, name);
            CheckResultStatus(status);
            return new Database(this, name);
        }

        public void RenameDatabase(ulong databaseID, string name)
        {
            int status = scaliendb_client.SDBP_RenameDatabase(cptr, databaseID, name);
            CheckResultStatus(status);
        }

        public void DeleteDatabase(ulong databaseID)
        {
            int status = scaliendb_client.SDBP_DeleteDatabase(cptr, databaseID);
            CheckResultStatus(status);
        }

        public void UseDatabaseID(ulong databaseID)
        {
            int status = scaliendb_client.SDBP_UseDatabaseID(cptr, databaseID);
            CheckStatus(status);
        }

        public void UseDatabase(string name)
        {
            int status = scaliendb_client.SDBP_UseDatabase(cptr, name);
            CheckStatus(status);
        }

        public ulong GetDatabaseID()
        {
            return scaliendb_client.SDBP_GetCurrentDatabaseID(cptr);
        }

        public ulong GetTableID()
        {
            return scaliendb_client.SDBP_GetCurrentTableID(cptr);
        }

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

        public Database GetDatabase(string name)
        {
            return new Database(this, name);
        }

        public ulong CreateTable(ulong databaseID, ulong quorumID, string name)
        {
            int status = scaliendb_client.SDBP_CreateTable(cptr, databaseID, quorumID, name);
            CheckResultStatus(status);
            return result.GetNumber();
        }

        public void RenameTable(ulong tableID, string name)
        {
            int status = scaliendb_client.SDBP_RenameTable(cptr, tableID, name);
            CheckResultStatus(status);
        }

        public void DeleteTable(ulong tableID)
        {
            int status = scaliendb_client.SDBP_DeleteTable(cptr, tableID);
            CheckResultStatus(status);
        }

        public void TruncateTable(ulong tableID)
        {
            int status = scaliendb_client.SDBP_TruncateTable(cptr, tableID);
            CheckResultStatus(status);
        }

        public void UseTable(String name)
        {
            int status = scaliendb_client.SDBP_UseTable(cptr, name);
            CheckStatus(status);
        }

        public void UseTableID(ulong tableID)
        {
            int status = scaliendb_client.SDBP_UseTableID(cptr, tableID);
            CheckStatus(status);
        }

        public Result GetResult()
        {
            lastResult = result;
            return result;
        }

        public string Get(string key)
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

        public byte[] Get(byte[] key)
        {
            int status = scaliendb_client.SDBP_GetCStr(cptr, key, key.Length);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            // TODO: fix SWIG wrapper and use GetValueBytes instead
            return StringToByteArray(result.GetValue());
        }

        public void Set(string key, string value)
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

        public void Set(byte[] key, byte[] value)
        {
            int status = scaliendb_client.SDBP_SetCStr(cptr, key, key.Length, value, value.Length);
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

        public long Add(string key, long value)
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

        public long Add(byte[] key, long value)
        {
            int status = scaliendb_client.SDBP_AddCStr(cptr, key, key.Length, value);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return result.GetSignedNumber();
        }

        public void Delete(string key)
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

        public void Delete(byte[] key)
        {
            int status = scaliendb_client.SDBP_DeleteCStr(cptr, key, key.Length);
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

        internal List<string> ListKeys(string startKey, string endKey, string prefix, uint count, bool skip)
        {
            int status = scaliendb_client.SDBP_ListKeys(cptr, startKey, endKey, prefix, count, skip);
            CheckResultStatus(status);
            return result.GetStringKeys();
        }

        internal List<byte[]> ListKeys(byte[] startKey, byte[] endKey, byte[] prefix, uint count, bool skip)
        {
            int status = scaliendb_client.SDBP_ListKeysCStr(cptr, startKey, startKey.Length, endKey, endKey.Length, prefix, prefix.Length, count, skip);
            CheckResultStatus(status);
            return result.GetByteKeys();
        }

        internal Dictionary<string, string> ListKeyValues(string startKey, string endKey, string prefix, uint count, bool skip)
        {
            int status = scaliendb_client.SDBP_ListKeyValues(cptr, startKey, endKey, prefix, count, skip);
            CheckResultStatus(status);
            return result.GetStringKeyValues();
        }

        internal Dictionary<byte[], byte[]> ListKeyValues(byte[] startKey, byte[] endKey, byte[] prefix, uint count, bool skip)
        {
            int status = scaliendb_client.SDBP_ListKeyValuesCStr(cptr, startKey, startKey.Length, endKey, endKey.Length, prefix, prefix.Length, count, skip);
            CheckResultStatus(status);
            return result.GetByteKeyValues();
        }
        
        public ulong Count(string startKey, string endKey, string prefix)
        {
            int status = scaliendb_client.SDBP_Count(cptr, startKey, endKey, prefix);
            CheckResultStatus(status);
            return result.GetNumber();
        }

        public ulong Count(byte[] startKey, byte[] endKey, byte[] prefix)
        {
            int status = scaliendb_client.SDBP_CountCStr(cptr, startKey, startKey.Length, endKey, endKey.Length, prefix, prefix.Length);
            CheckResultStatus(status);
            return result.GetNumber();
        }

        // foreach (string k in client.GetKeyIterator())
        //      System.Console.WriteLine(k);
        public StringKeyIterator GetKeyIterator(StringIterParams ps)
        {
            return new StringKeyIterator(this, ps);
        }

        public ByteKeyIterator GetKeyIterator(ByteIterParams ps)
        {
            return new ByteKeyIterator(this, ps);
        }

        // foreach (KeyValuePair<string, string> kv in client.GetKeyValueIterator())
        //     System.Console.WriteLine(kv.Key + " => " + kv.Value);
        public StringKeyValueIterator GetKeyValueIterator(StringIterParams ps)
        {
            return new StringKeyValueIterator(this, ps);
        }

        public ByteKeyValueIterator GetKeyValueIterator(ByteIterParams ps)
        {
            return new ByteKeyValueIterator(this, ps);
        }

        // Index ind = client.GetIndex("ind");
        // ulong i = personIndex.Get
        public Index GetIndex(string key)
        {
            return new Index(this, GetDatabaseID(), GetTableID(), key);
        }

        public SubmitGuard Begin()
        {
            return new SubmitGuard(this);
        }

        public void Submit()
        {
            int status = scaliendb_client.SDBP_Submit(cptr);
            CheckStatus(status);
        }

        public void Cancel()
        {
            int status = scaliendb_client.SDBP_Cancel(cptr);
            CheckStatus(status);
        }

        // TODO:
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

        public static void SetTrace(bool trace)
        {
            scaliendb_client.SDBP_SetTrace(trace);
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


    }
}
