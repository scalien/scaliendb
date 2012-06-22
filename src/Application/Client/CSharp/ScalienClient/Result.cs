using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.InteropServices;
using System.Threading;

namespace Scalien
{
    class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        public bool Equals(byte[] left, byte[] right)
        {
            if (left == null || right == null)
                return left == right;

            if (left.Length != right.Length) 
                return false;

            for (int i= 0; i < left.Length; i++) {
                if (left[i] != right[i])
                    return false;

            }
            return true;
        }
        public int GetHashCode(byte[] key)
        {
            if (key == null)
                throw new ArgumentNullException("key");
        
            int sum = 0;
            foreach (byte cur in key)
                sum = sum * 37 + cur;
            return sum;
        }
    }

    internal class Result
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
            var oldPtr = Interlocked.Exchange(ref cptr, null);
            if (oldPtr != null)
            {
                scaliendb_client.SDBP_ResultClose(oldPtr);
            }
        }

        public string GetKey()
        {
            return scaliendb_client.SDBP_ResultKey(cptr);
        }

        public byte[] GetKeyBytes()
        {
            return GetBytes(scaliendb_client.SDBP_ResultKeyBuffer(cptr));
        }

        public string GetValue()
        {
            return scaliendb_client.SDBP_ResultValue(cptr);
        }

        public byte[] GetValueBytes()
        {
            return GetBytes(scaliendb_client.SDBP_ResultValueBuffer(cptr));
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

        public List<byte[]> GetByteArrayKeys()
        {
            List<byte[]> keys = new List<byte[]>();
            for (Begin(); !IsEnd(); Next())
                keys.Add(GetKeyBytes());
            return keys;
        }

        public Dictionary<string, string> GetStringKeyValues()
        {
            Dictionary<string, string> keyvals = new Dictionary<string, string>();
            for (Begin(); !IsEnd(); Next())
                keyvals.Add(GetKey(), GetValue());

            return keyvals;
        }

        public Dictionary<byte[], byte[]> GetByteArrayKeyValues()
        {
            // TODO: we should return a sorted dictionary
            Dictionary<byte[], byte[]> keyvals = new Dictionary<byte[], byte[]>(new ByteArrayComparer());
            for (Begin(); !IsEnd(); Next())
                keyvals.Add(GetKeyBytes(), GetValueBytes());

            return keyvals;
        }

        private delegate UInt64 ResultUIn64Getter(SWIGTYPE_p_void result);
        private UInt64? GetID(ResultUIn64Getter getter, UInt64 unknownValue)
        {
            UInt64 retval = getter(cptr);
            if (retval == unknownValue)
                return null;
            
            return retval;
        }

        public UInt64? GetNodeID()
        {
            return GetID(scaliendb_client.SDBP_ResultNodeID, UInt64.MaxValue);
        }

        public UInt64? GetQuorumID()
        {
            return GetID(scaliendb_client.SDBP_ResultQuorumID, 0);
        }

        public UInt64? GetTableID() 
        {
            return GetID(scaliendb_client.SDBP_ResultTableID, 0);
        }

        public UInt64? GetDatabaseID()
        {
            return GetID(scaliendb_client.SDBP_ResultDatabaseID, 0);
        }

        public UInt64? GetPaxosID()
        {
            return GetID(scaliendb_client.SDBP_ResultPaxosID, 0);
        }

        public uint GetElapsedTime()
        {
            return scaliendb_client.SDBP_ResultElapsedTime(cptr);
        }

        private byte[] GetBytes(SDBP_Buffer buffer)
        {
            unsafe
            {
                byte[] result = new byte[buffer.len];
                Marshal.Copy(buffer.data, result, 0, result.Length);
                return result;
            }
        }
    }
}
