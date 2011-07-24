using System;
using System.Collections.Generic;

namespace Scalien
{
    public class Index
    {
        Client client;
        Table table;
        ulong databaseID;
        ulong tableID;
        string key;
        long count;

        long index;
        long num;

        public Index(Client client, ulong databaseID, ulong tableID, string key)
        {
            this.client = client;
            this.databaseID = databaseID;
            this.tableID = tableID;
            this.key = key;
            this.index = 0;
            this.count = 100;
            this.num = 0;
        }

        public Index(Table table, string key)
        {
            this.table = table;
            this.key = key;
            this.count = 100;
            this.index = 0;
            this.num = 0;
        }

        public ulong Get
        {
            get
            {
                if (num == 0)
                {
                    Client client = GetClient();

                    // retrieve new indices
                    ulong oldDatabaseID = client.GetDatabaseID();
                    ulong oldTableID = client.GetTableID();

                    if (table != null)
                    {
                        index = table.Add(key, count) - count;
                    }
                    else
                    {
                        client.UseDatabaseID(databaseID);
                        client.UseTableID(tableID);
                        index = client.Add(key, count) - count;
                    }
                    
                    num = count;

                    client.UseDatabaseID(oldDatabaseID);
                    client.UseTableID(oldTableID);
                }

                if (num == 0)
                    throw new Exception("ScalienDB: index error");
                
                num--;
                return (ulong)index++;
            }
        }

        private Client GetClient()
        {
            if (this.client != null)
                return this.client;
            else
                return table.Client;
        }
    }
}
