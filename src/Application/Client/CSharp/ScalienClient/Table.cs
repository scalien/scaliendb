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

        public void Set(string key, string value)
        {
            UseDefaults();
            client.Set(key, value);
        }

        public bool SetIfNotExists(string key, string value)
        {
            UseDefaults();
            return client.SetIfNotExists(key, value);
        }

        public bool TestAndSet(string key, string test, string value)
        {
            UseDefaults();
            return client.TestAndSet(key, test, value);
        }

        public string GetAndSet(string key, string value)
        {
            UseDefaults();
            return client.GetAndSet(key, value);
        }

        public long Add(string key, long number)
        {
            UseDefaults();
            return client.Add(key, number);
        }

        public void Append(string key, string value)
        {
            UseDefaults();
            client.Append(key, value);
        }

        public void Delete(string key)
        {
            UseDefaults();
            client.Delete(key);
        }

        public bool TestAndDelete(string key, string test)
        {
            UseDefaults();
            return client.TestAndDelete(key, test);
        }

        public string Remove(string key)
        {
            UseDefaults();
            return client.Remove(key);
        }

        public List<string> ListKeys(string startKey, string endKey, string prefix, uint offset, uint count)
        {
            UseDefaults();
            return client.ListKeys(startKey, endKey, prefix, offset, count);
        }

        public Dictionary<string, string> ListKeyValues(string startKey, string endKey, string prefix, uint offset, uint count)
        {
            UseDefaults();
            return client.ListKeyValues(startKey, endKey, prefix, offset, count);
        }

        public ulong Count(string startKey, string endKey, string prefix, uint offset, uint count)
        {
            UseDefaults();
            return client.Count(startKey, endKey, prefix, offset, count);
        }
    }
}
