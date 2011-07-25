using System;
using System.Collections.Generic;
using System.Text;

namespace Scalien
{
    public class Table
    {
        private Client client;
        private Database database;
        private string name;
        private ulong tableID;

        public Table(Client client, Database database, string name)
        {
            this.client = client;
            this.database = database;
            this.name = name;

            tableID = scaliendb_client.SDBP_GetTableID(client.cptr, database.GetDatabaseID(), name);
            if (tableID == 0)
                throw new SDBPException(Status.SDBP_BADSCHEMA);
        }

        private void UseDefaults()
        {
            client.UseDatabaseID(database.GetDatabaseID());
            client.UseTableID(tableID);
        }

        public void RenameTable(string newName)
        {
            client.RenameTable(tableID, newName);
        }

        public void DeleteTable()
        {
            client.DeleteTable(tableID);
        }

        public void TruncateTable()
        {
            client.TruncateTable(tableID);
        }

        public string Get(string key)
        {
            UseDefaults();
            return client.Get(key);
        }

        public byte[] Get(byte[] key)
        {
            UseDefaults();
            return client.Get(key);
        }

        public void Set(string key, string value)
        {
            UseDefaults();
            client.Set(key, value);
        }

        public void Set(byte[] key, byte[] value)
        {
            UseDefaults();
            client.Set(key, value);
        }

        public long Add(string key, long value)
        {
            UseDefaults();
            return client.Add(key, value);
        }

        public long Add(byte[] key, long value)
        {
            UseDefaults();
            return client.Add(key, value);
        }

        public void Delete(string key)
        {
            UseDefaults();
            client.Delete(key);
        }

        public void Delete(byte[] key)
        {
            UseDefaults();
            client.Delete(key);
        }

        public List<string> ListKeys(string startKey, string endKey, string prefix, uint count, bool skip)
        {
            UseDefaults();
            return client.ListKeys(startKey, endKey, prefix, count, skip);
        }

        public Dictionary<string, string> ListKeyValues(string startKey, string endKey, string prefix, uint count, bool skip)
        {
            UseDefaults();
            return client.ListKeyValues(startKey, endKey, prefix, count, skip);
        }

        public ulong Count(string startKey, string endKey, string prefix)
        {
            UseDefaults();
            return client.Count(startKey, endKey, prefix);
        }

        public StringKeyIterator GetKeyIterator(StringIterParams ps)
        {
            return new StringKeyIterator(this, ps);
        }

        public StringKeyValueIterator GetKeyValueIterator(StringIterParams ps)
        {
            return new StringKeyValueIterator(this, ps);
        }

        public Index GetIndex(string key)
        {
            return new Index(this, key);
        }

        public Client Client
        {
            get
            {
                return client;
            }
        }
    }
}
