using System;
using System.Collections.Generic;

namespace Scalien
{
    public class Index
    {
        Client client;
        ulong databaseID;
        ulong tableID;
        string stringKey;
        byte[] byteKey;

        long count = 1000;
        long index = 0;
        long num = 0;

        public Index(Client client, ulong databaseID, ulong tableID, string key)
        {
            this.client = client;
            this.databaseID = databaseID;
            this.tableID = tableID;
            this.stringKey = key;
        }

        public Index(Client client, ulong databaseID, ulong tableID, byte[] key)
        {
            this.client = client;
            this.databaseID = databaseID;
            this.tableID = tableID;
            this.byteKey = key;
        }

        public void SetGranularity(long count)
        {
            this.count = count;
        }

        public long Get
        {
            get
            {
                if (num == 0)
                    AllocateIndexRange();
                
                num--;
                return index++;
            }
        }

        private void AllocateIndexRange()
        {
            ulong oldDatabaseID = client.GetDatabaseID();
            ulong oldTableID = client.GetTableID();

            if (oldDatabaseID != databaseID)
                client.UseDatabaseID(databaseID);
            if (oldTableID != tableID)
                client.UseTableID(tableID);

            if (stringKey != null)
                index = client.Add(stringKey, count) - count;
            else
                index = client.Add(byteKey, count) - count;
            num = count;

            if (oldDatabaseID != databaseID)
                client.UseDatabaseID(oldDatabaseID);
            if (oldTableID != tableID)
                client.UseTableID(oldTableID);
        }
    }
}
