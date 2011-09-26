using System;
using System.Collections;
using System.Collections.Generic;

namespace Scalien
{
    public class ByteKeyIterator : IEnumerable<byte[]>, IEnumerator<byte[]>
    {
        Table table;
        byte[] startKey;
        byte[] endKey;
        byte[] prefix;
        long count;
        uint gran = 100;
        int pos;
        List<byte[]> keys;

        public ByteKeyIterator(Table table, ByteRangeParams ps)
        {
            this.table = table;
            this.startKey = ps.startKey;
            this.endKey = ps.endKey;
            this.prefix = ps.prefix;
            this.count = ps.count;

            Query(false);
        }

        private void Query(bool skip)
        {
            uint num;

            num = gran;
            if (count > 0 && count < gran)
                num = (uint)count;

            keys = table.Client.ListKeys(table.TableID, startKey, endKey, prefix, num, skip);
            pos = 0;
        }

        #region IEnumerable interface

        IEnumerator IEnumerable.GetEnumerator()
        {
            return (IEnumerator)GetEnumerator();
        }

        public IEnumerator<byte[]> GetEnumerator()
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

        public byte[] Current
        {
            get
            {
                return keys[pos - 1];
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
