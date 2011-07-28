using System;
using System.Collections.Generic;

namespace Scalien
{
    public class Index
    {
        Client client;
        ulong databaseID;
        ulong tableID;
        string key;

        long count = 1000;
        long index = 0;
        long num = 0;

        public Index(Client client, ulong databaseID, ulong tableID, string key)
        {
            this.client = client;
            this.databaseID = databaseID;
            this.tableID = tableID;
            this.key = key;
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

            index = client.Add(key, count) - count;
            num = count;

            if (oldDatabaseID != databaseID)
                client.UseDatabaseID(oldDatabaseID);
            if (oldTableID != tableID)
                client.UseTableID(oldTableID);
        }
    }
}
