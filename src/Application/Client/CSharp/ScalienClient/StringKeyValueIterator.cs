using System;
using System.Collections;
using System.Collections.Generic;

namespace Scalien
{
    public class StringKeyValueIterator : IEnumerable<KeyValuePair<string, string>>, IEnumerator<KeyValuePair<string, string>>
    {
        Table table;
        bool forwardDirection; 
        string startKey;
        string endKey;
        string prefix;
        long count;
        uint granularity = 100;
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
            this.forwardDirection = ps.forwardDirection;
            this.granularity = ps.granularity;
            Query(false);
        }

        private void Query(bool skip)
        {
            uint num;
            Dictionary<string, string> result;

            num = granularity;
            if (count > 0 && count < granularity)
                num = (uint)count;

            result = table.Client.ListKeyValues(table.TableID, startKey, endKey, prefix, num, forwardDirection, skip);

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

        public virtual IEnumerator<KeyValuePair<string, string>> GetEnumerator()
        {
            return this;
        }

        #endregion

        #region IEnumerator interface

        public virtual bool MoveNext()
        {
            if (count == 0)
                return false;
            if (pos == keys.Count)
            {
                if (keys.Count < granularity)
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

        public virtual void Reset()
        {
            throw new NotSupportedException();
        }

        public virtual KeyValuePair<string, string> Current
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
