using System;
using System.Collections;
using System.Collections.Generic;

namespace Scalien
{
    public class StringKeyValueIterator : IEnumerable<KeyValuePair<string, string>>, IEnumerator<KeyValuePair<string, string>>
    {
        Client client;
        Table table;
        string startKey;
        string endKey;
        string prefix;
        uint count;
        int pos;
        List<string> keys;
        List<string> values;

        public StringKeyValueIterator(Client client, string startKey, string endKey, string prefix)
        {
            this.client = client;
            this.startKey = startKey;
            this.endKey = endKey;
            this.prefix = prefix;
            this.count = 100;

            Query(false);
        }

        public StringKeyValueIterator(Table table, string startKey, string endKey, string prefix)
        {
            this.table = table;
            this.startKey = startKey;
            this.endKey = endKey;
            this.prefix = prefix;
            this.count = 100;

            Query(false);
        }

        private void Query(bool skip)
        {
            Dictionary<string, string> result;
            if (client != null)
                result = client.ListKeyValues(startKey, endKey, prefix, count, skip);
            else
                result = table.ListKeyValues(startKey, endKey, prefix, count, skip);

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
