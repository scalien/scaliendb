using System;
using System.Collections.Generic;
using System.Text;

namespace Scalien
{
    public class Database
    {
        private Client client;
        private string name;
        private ulong databaseID;

        public ulong DatabaseID
        {
            get
            {
                return databaseID;
            }
        }

        public Database(Client client, string name)
        {
            this.client = client;
            this.name = name;

            databaseID = scaliendb_client.SDBP_GetDatabaseID(client.cptr, name);
            if (databaseID == 0)
                throw new SDBPException(Status.SDBP_BADSCHEMA);
        }

        public string GetName()
        {
            return name;
        }

        public List<Table> GetTables()
        {
            ulong numTables = scaliendb_client.SDBP_GetNumTables(client.cptr);
            List<Table> tables = new List<Table>();
            for (uint i = 0; i < numTables; i++)
            {
                string name = scaliendb_client.SDBP_GetTableNameAt(client.cptr, i);
                tables.Add(new Table(client, this, name));
            }
            return tables;
        }

        public Table GetTable(string name)
        {
            return new Table(client, this, name);
        }

        public Table CreateTable(Quorum quorum, string name)
        {
            ulong createdTableID = client.CreateTable(databaseID, quorum.GetQuorumID(), name);
            ulong numTables = scaliendb_client.SDBP_GetNumTables(client.cptr);
            for (uint i = 0; i < numTables; i++)
            {
                ulong tableID = scaliendb_client.SDBP_GetTableIDAt(client.cptr, i);
                if (tableID == createdTableID)
                    return new Table(client, this, name);
            }
            throw new SDBPException(Status.SDBP_API_ERROR, "Cannot find created table");
        }

        public void RenameDatabase(string newName)
        {
            client.RenameDatabase(databaseID, name);
        }

        public void DeleteDatabase()
        {
            client.DeleteDatabase(databaseID);
        }
    }
}
