using System;
using System.Collections.Generic;
using System.Text;

namespace Scalien
{
    /// <summary>
    /// Database is a convenience class for encapsulating database related operations.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ScalienDB uses databases and tables to manage key value namespaces.
    /// </para>
    /// </remarks>
    /// <example><code>
    /// db = client.GetDatabase("testDatabase");
    /// table = db.GetTable("testTable");
    /// table.Set("foo", "bar");
    /// </code></example>
    /// <seealso cref="Client.CreateDatabase(string)"/>
    /// <seealso cref="Client.GetDatabase(string)"/>
    /// <seealso cref="Table"/>
    /// <seealso cref="Quorum"/>
    public class Database
    {
        private Client client;
        private string name;
        private ulong databaseID;

        #region Properties

        internal ulong DatabaseID
        {
            get
            {
                return databaseID;
            }
        }

        /// <summary>
        /// The name of the database.
        /// </summary>
        public string Name
        {
            get
            {
                return name;
            }
        }

        #endregion

        #region Constructors, destructors

        internal Database(Client client, ulong databaseID, string name)
        {
            this.client = client;
            this.databaseID = databaseID;
            this.name = name;
        }

        #endregion

        /// <summary>
        /// Retrieve the tables in the database as a list of <see cref="Scalien.Table"/> objects.
        /// </summary>
        /// <returns>The list of table objects.</returns>
        /// <seealso cref="Table"/>
        /// <exception cref="SDBPException"/>
        public List<Table> GetTables()
        {
            ulong numTables = scaliendb_client.SDBP_GetNumTables(client.cptr, databaseID);
            List<Table> tables = new List<Table>();
            for (uint i = 0; i < numTables; i++)
            {
                ulong tableID = scaliendb_client.SDBP_GetTableIDAt(client.cptr, databaseID, i);
                string name = scaliendb_client.SDBP_GetTableNameAt(client.cptr, databaseID, i);
                tables.Add(new Table(client, this, tableID, name));
            }
            return tables;
        }

        /// <summary>
        /// Retrieve a <see cref="Scalien.Table"/> in this database by name.
        /// </summary>
        /// <param name="name">The name of the table.</param>
        /// <returns>The corresponding <see cref="Scalien.Table"/> object.</returns>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Scalien.Table"/>
        public Table GetTable(string name)
        {
            List<Table> tables = GetTables();
            foreach (Table table in tables)
            {
                if (table.Name == name)
                    return table;
            }

            throw new SDBPException(Status.SDBP_BADSCHEMA);
        }

        /// <summary>
        /// Create a table in this database, with the first shard placed in the first available quorum.
        /// </summary>
        /// <param name="name">The name of the table.</param>
        /// <returns>The <see cref="Scalien.Table"/> object corresponding to the created table.</returns>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Scalien.Table"/>
        /// <seealso cref="CreateTable(string, Quorum)"/>
        public Table CreateTable(string name)
        {
            List<Quorum> quorums = client.GetQuorums();
            if (quorums.Count == 0)
                throw new SDBPException(Status.SDBP_BADSCHEMA, "No quorums found");
            Quorum quorum = quorums[0];
            return CreateTable(name, quorum);
        }

        /// <summary>
        /// Create a table in this database.
        /// </summary>
        /// <param name="quorum">The first shard of the table is placed inside the <see cref="Scalien.Quorum"/>.</param>
        /// <param name="name">The name of the table.</param>
        /// <returns>The <see cref="Scalien.Table"/> object corresponding to the created table.</returns>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="CreateTable(string)"/>
        public Table CreateTable(string name, Quorum quorum)
        {
            int status = scaliendb_client.SDBP_CreateTable(client.cptr, databaseID, quorum.QuorumID, name);
            client.CheckResultStatus(status);
            return GetTable(name);
        }

        /// <summary>
        /// Rename the database.
        /// </summary>
        /// <param name="newName">The new database name.</param>
        /// <exception cref="SDBPException"/>
        public void RenameDatabase(string newName)
        {
            int status = scaliendb_client.SDBP_RenameDatabase(client.cptr, databaseID, newName);
            client.CheckResultStatus(status);
        }

        /// <summary>
        /// Delete the database.
        /// </summary>
        /// <remarks>Do not use the database object after calling DeleteDatabase().</remarks>
        /// <exception cref="SDBPException"/>
        public void DeleteDatabase()
        {
            int status = scaliendb_client.SDBP_DeleteDatabase(client.cptr, databaseID);
            client.CheckResultStatus(status);
        }
    }
}
