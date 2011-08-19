using System;
using System.Collections;
using System.Collections.Generic;

namespace Scalien
{
    public class StringKeyValueIterator : IEnumerable<KeyValuePair<string, string>>, IEnumerator<KeyValuePair<string, string>>
    {
        Table table;
        string startKey;
        string endKey;
        string prefix;
        long count;
        uint gran;
        int pos;
        List<string> keys;
        List<string> values;

        public StringKeyValueIterator(Table table, StringRangeParams ps)
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
            Dictionary<string, string> result;

            num = gran;
            if (count > 0 && count < gran)
                num = (uint)count;

            result = table.Client.ListKeyValues(table.TableID, startKey, endKey, prefix, num, skip);

            keys = new List<string>();
            values = new List<string>();

            foreach (KeyValuePair<string, string> kv in result)
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

        public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
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

        public KeyValuePair<string, string> Current
        {
            get
            {
                return new KeyValuePair<string,string>(keys[pos - 1], values[pos - 1]);
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
