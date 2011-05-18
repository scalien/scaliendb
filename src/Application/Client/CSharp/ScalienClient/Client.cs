using System;
using System.Collections.Generic;
using System.Text;

namespace Scalien
{
    public class SDBPException : ApplicationException
    {
        public SDBPException(string msg)
        {
            //base(msg);   
        }
    }

    public class Client
    {
        private SWIGTYPE_p_void cptr;
        private Result result;

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

        public Result GetResult()
        {
            return result;
        }

        public void UseDatabase(string name)
        {
            scaliendb_client.SDBP_UseDatabase(cptr, name);
        }

        public void UseTable(string name)
        {
            scaliendb_client.SDBP_UseTable(cptr, name);
        }

        public string Get(string key)
        {
            int status = scaliendb_client.SDBP_Get(cptr, key);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                throw new Exception(Status.ToString(status));
            }

            if (IsBatched())
                return null;

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return result.GetValue();
        }

        public int Set(string key, string value)
        {
            int status = scaliendb_client.SDBP_Set(cptr, key, value);
            if (status < 0)
            {
                result = new Result(scaliendb_client.SDBP_GetResult(cptr));
                throw new Exception(Status.ToString(status));
            }

            if (IsBatched())
                return status;

            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return status;
        }


        public bool IsBatched()
        {
            return scaliendb_client.SDBP_IsBatched(cptr);
        }

        public static void SetTrace(bool trace)
        {
            scaliendb_client.SDBP_SetTrace(trace);
        }
    }
}
