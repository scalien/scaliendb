using System;
using System.Collections.Generic;
using System.Text;

namespace Scalien
{
    public class Result
    {
        private SWIGTYPE_p_void cptr;

        public Result(SWIGTYPE_p_void cptr)
        {
            this.cptr = cptr;
        }

        ~Result()
        {
            Close();
        }

        public void Close()
        {
            scaliendb_client.SDBP_ResultClose(cptr);
            cptr = null;
        }

        public string GetKey()
        {
            return scaliendb_client.SDBP_ResultKey(cptr);
        }

        public string GetValue()
        {
            return scaliendb_client.SDBP_ResultValue(cptr);
        }

        public long GetSignedNumber()
        {
            return scaliendb_client.SDBP_ResultSignedNumber(cptr);
        }

        public ulong GetNumber()
        {
            return scaliendb_client.SDBP_ResultNumber(cptr);
        }

        public bool IsConditionalSuccess()
        {
            return scaliendb_client.SDBP_ResultIsConditionalSuccess(cptr);
        }

        public void Begin()
        {
            scaliendb_client.SDBP_ResultBegin(cptr);
        }

        public void Next()
        {
            scaliendb_client.SDBP_ResultNext(cptr);
        }

        public bool IsEnd()
        {
            return scaliendb_client.SDBP_ResultIsEnd(cptr);
        }

        public int GetTransportStatus()
        {
            return scaliendb_client.SDBP_ResultTransportStatus(cptr);
        }

        public int GetCommandStatus()
        {
            return scaliendb_client.SDBP_ResultCommandStatus(cptr);
        }

        public List<string> GetStringKeys()
        {
            List<string> keys = new List<string>();
            for (Begin(); !IsEnd(); Next())
                keys.Add(GetKey());

            return keys;
        }

        public List<byte[]> GetByteKeys()
        {
            List<byte[]> keys = new List<byte[]>();
            for (Begin(); !IsEnd(); Next())
                keys.Add(Client.StringToByteArray(GetKey()));

            return keys;
        }

        public Dictionary<string, string> GetStringKeyValues()
        {
            Dictionary<string, string> keyvals = new Dictionary<string, string>();
            for (Begin(); !IsEnd(); Next())
                keyvals.Add(GetKey(), GetValue());

            return keyvals;
        }
        public Dictionary<byte[], byte[]> GetByteKeyValues()
        {
            Dictionary<byte[], byte[]> keyvals = new Dictionary<byte[], byte[]>(new Client.ByteArrayComparer());
            for (Begin(); !IsEnd(); Next())
                keyvals.Add(Client.StringToByteArray(GetKey()), Client.StringToByteArray(GetValue()));

            return keyvals;
        }
    }
}
