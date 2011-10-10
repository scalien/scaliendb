using System;
using System.Collections;
using System.Collections.Generic;

namespace Scalien
{
    public class ByteKeyValueIterator : IEnumerable<KeyValuePair<byte[], byte[]>>, IEnumerator<KeyValuePair<byte[], byte[]>>
    {
        Table table;
        byte[] startKey;
        byte[] endKey;
        byte[] prefix;
        long count;
        bool forwardDirection;
        uint gran = 100;
        int pos;
        List<byte[]> keys;
        List<byte[]> values;

        public ByteKeyValueIterator(Table table, ByteRangeParams ps)
        {
            this.table = table;
            this.startKey = ps.startKey;
            this.endKey = ps.endKey;
            this.prefix = ps.prefix;
            this.count = ps.count;
            this.forwardDirection = ps.forwardDirection;

            Query(false);
        }

        private void Query(bool skip)
        {
            uint num;
            Dictionary<byte[], byte[]> result;

            num = gran;
            if (count > 0 && count < gran)
                num = (uint)count;
            
            result = table.Client.ListKeyValues(table.TableID, startKey, endKey, prefix, num, forwardDirection, skip);

            keys = new List<byte[]>();
            values = new List<byte[]>();

            foreach (KeyValuePair<byte[], byte[]> kv in result)
            {
                keys.Add(kv.Key);
                values.Add(kv.Value);
            }

            pos = 0;
        }

        #region IEnumerable interface

        IEnumerator IEnumerable.GetEnumerator()
        {
            return (IEnumerator)GetEnumerator();
        }

        public IEnumerator<KeyValuePair<byte[], byte[]>> GetEnumerator()
        {
            return this;
        }

        #endregion

        #region IEnumerator interface

        public bool MoveNext()
        {
            if (count == 0)
                return false;
            if (pos == keys.Count)
            {
                if (keys.Count < gran)
                    return false;
                startKey = keys[keys.Count - 1];
                Query(true);
            }
            if (keys.Count == 0)
                return false;

            pos++;
            if (count > 0)
                count--;
            return true;
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        public KeyValuePair<byte[], byte[]> Current
        {
            get
            {
                return new KeyValuePair<byte[], byte[]>(keys[pos - 1], values[pos - 1]);
            }
        }

        object IEnumerator.Current
        {
            get
            {
                return Current;
            }
        }

        void IDisposable.Dispose()
        {
        }

        #endregion
    }
}
