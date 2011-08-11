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
        uint count;
        int pos;
        List<byte[]> keys;

        public ByteKeyIterator(Table table, ByteRangeParams ps)
        {
            this.table = table;
            this.startKey = ps.startKey;
            this.endKey = ps.endKey;
            this.prefix = ps.prefix;
            this.count = 100;

            Query(false);
        }

        private void Query(bool skip)
        {
            keys = table.Client.ListKeys(table.TableID, startKey, endKey, prefix, count, skip);
            keys.Sort();
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
            if (pos == keys.Count)
            {
                if (keys.Count < count)
                    return false;
                startKey = keys[keys.Count - 1];
                Query(true);
            }
            if (keys.Count == 0)
                return false;

            pos++;
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
