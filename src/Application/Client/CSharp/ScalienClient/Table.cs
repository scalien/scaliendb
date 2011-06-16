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
    }
}
