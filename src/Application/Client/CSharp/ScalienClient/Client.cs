using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;

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

        public SDBPException(int status) : base(Scalien.Status.ToString(status))
        {
            this.status = status;
        }

        public SDBPException(int status, string msg) : base(msg)
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

        public void SetBulkLoading(bool bulk)
        {
            scaliendb_client.SDBP_SetBulkLoading(cptr, bulk);
        }

        public void SetConsistencyLevel(int consistencyLevel)
        {
            scaliendb_client.SDBP_SetConsistencyLevel(cptr, consistencyLevel);
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

            if (IsBatched())
                return null;

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return result.GetValue();
        }

        public string Get(byte[] key)
        {
            int status = scaliendb_client.SDBP_GetCStr(cptr, key, key.Length);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            if (IsBatched())
                return null;

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return result.GetValue();
        }

        public void Set(string key, string value)
        {
            int status = scaliendb_client.SDBP_Set(cptr, key, value);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            if (IsBatched())
                return;

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        }

        public void Set(byte[] key, byte[] value)
        {
            int status = scaliendb_client.SDBP_SetCStr(cptr, key, key.Length, value, value.Length);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            if (IsBatched())
                return;

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        }

        public bool SetIfNotExists(string key, string value)
        {
            int status = scaliendb_client.SDBP_SetIfNotExists(cptr, key, value);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            if (IsBatched())
                return false;

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            if (result.GetCommandStatus() == Status.SDBP_SUCCESS)
                return true;
            return false;
        }

        public bool TestAndSet(string key, string test, string value)
        {
            int status = scaliendb_client.SDBP_TestAndSet(cptr, key, test, value);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            if (IsBatched())
                return false;

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return result.IsConditionalSuccess() ;
        }

        public string GetAndSet(string key, string value)
        {
            int status = scaliendb_client.SDBP_GetAndSet(cptr, key, value);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            if (IsBatched())
                return null;

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return result.GetValue();
        }

        public long Add(string key, long number)
        {
            int status = scaliendb_client.SDBP_Add(cptr, key, number);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            if (IsBatched())
                return 0;

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return result.GetSignedNumber();
        }

        public void Append(string key, string value)
        {
            int status = scaliendb_client.SDBP_Append(cptr, key, value);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            if (IsBatched())
                return;

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        }

        public void Delete(string key)
        {
            int status = scaliendb_client.SDBP_Delete(cptr, key);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            if (IsBatched())
                return;

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        }

        public bool TestAndDelete(string key, string test)
        {
            int status = scaliendb_client.SDBP_TestAndDelete(cptr, key, test);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            if (IsBatched())
                return false;

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return result.IsConditionalSuccess();
        }

        public string Remove(string key)
        {
            int status = scaliendb_client.SDBP_Delete(cptr, key);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                CheckStatus(status);
            }

            if (IsBatched())
                return null;

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            if (result.GetCommandStatus() == Status.SDBP_SUCCESS)
                return result.GetValue();
            return null;
        }

        public List<string> ListKeys(string startKey, string endKey, string prefix, uint offset, uint count)
        {
            int status = scaliendb_client.SDBP_ListKeys(cptr, startKey, endKey, prefix, count, offset);
            CheckResultStatus(status);
            return result.GetKeys();
        }

        public Dictionary<string, string> ListKeyValues(string startKey, string endKey, string prefix, uint offset, uint count)
        {
            int status = scaliendb_client.SDBP_ListKeyValues(cptr, startKey, endKey, prefix, count, offset);
            CheckResultStatus(status);
            return result.GetKeyValues();
        }

        public ulong Count(string startKey, string endKey, string prefix, uint offset, uint count)
        {
            int status = scaliendb_client.SDBP_Count(cptr, startKey, endKey, prefix, count, offset);
            CheckResultStatus(status);
            return result.GetNumber();
        }

        public void Begin()
        {
            if (lastResult != result)
                result.Close();

            result = null;
            lastResult = null;

            int status = scaliendb_client.SDBP_Begin(cptr);
            CheckStatus(status);
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

        public bool IsBatched()
        {
            return scaliendb_client.SDBP_IsBatched(cptr);
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
    }
}
