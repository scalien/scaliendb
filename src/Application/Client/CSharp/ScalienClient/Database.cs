using System;
using System.Collections.Generic;
using System.Text;

namespace Scalien
{
    /// <summary>
    /// Database is a convenience class for encapsulating database related operations.
    /// </summary>
    ///
    /// <remarks>
    /// <para>
    /// ScalienDB uses databases and tables to manage key value namespaces.
    /// </para>
    /// </remarks>
    /// 
    /// <example><code>
    /// db = client.GetDatabase("testDatabase");
    /// table = db.GetTable("testTable");
    /// table.Set("foo", "bar");
    /// </code></example>
    /// 
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

        internal Database(Client client, string name)
        {
            this.client = client;
            this.name = name;

            databaseID = scaliendb_client.SDBP_GetDatabaseID(client.cptr, name);
            if (databaseID == 0)
                throw new SDBPException(Status.SDBP_BADSCHEMA);
        }

        #endregion

        /// <summary>
        /// Retrieve the tables in the database as a list of <see cref="Scalien.Table"/> objects.
        /// </summary>
        /// 
        /// <returns>The list of table objects.</returns>
        /// <seealso cref="Table"/>
        /// <exception cref="SDBPException"/>
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

        /// <summary>
        /// Retrieve a <see cref="Scalien.Table"/> in this database by name.
        /// </summary>
        /// <param name="name">The name of the table.</param>
        /// <returns>The corresponding <see cref="Scalien.Table"/> object.</returns>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Scalien.Table"/>
        public Table GetTable(string name)
        {
            return new Table(client, this, name);
        }

        /// <summary>
        /// Create a table in this database, with the first shard placed in the first available quorum.
        /// </summary>
        /// <param name="name">The name of the table.</param>
        /// <returns>The <see cref="Scalien.Table"/> object corresponding to the created table.</returns>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Scalien.Table"/>
        /// <seealso cref="CreateTable(string, Quorum)"/>
        /// <seealso cref="Scalien.Client.CreateTable(string)"/>
        /// <seealso cref="Scalien.Client.CreateTable(string, string)"/>
        public Table CreateTable(string name)
        {
            ulong prevDatabaseID = client.GetDatabaseID();
            client.UseDatabaseID(this.DatabaseID);
            Table table = client.CreateTable(name);
            client.UseDatabaseID(prevDatabaseID);
            return table;
        }

        /// <summary>
        /// Create a table in this database.
        /// </summary>
        /// <param name="quorum">The first shard of the table is placed inside the <see cref="Scalien.Quorum"/>.</param>
        /// <param name="name">The name of the table.</param>
        /// <returns>The <see cref="Scalien.Table"/> object corresponding to the created table.</returns>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="CreateTable(string)"/>
        /// <seealso cref="Scalien.Client.CreateTable(string)"/>
        /// <seealso cref="Scalien.Client.CreateTable(string, string)"/>
        public Table CreateTable(string name, Quorum quorum)
        {
            ulong prevDatabaseID = client.GetDatabaseID();
            client.UseDatabaseID(this.DatabaseID);
            Table table = client.CreateTable(name, quorum.Name);
            client.UseDatabaseID(prevDatabaseID);
            return table;
        }

        /// <summary>
        /// Rename the database.
        /// </summary>
        /// <param name="newName">The new database name.</param>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Scalien.Client.RenameDatabase(string, string)"/>
        public void RenameDatabase(string newName)
        {
            client.RenameDatabase(this.Name, newName);
        }

        /// <summary>
        /// Delete the database.
        /// </summary>
        /// <remarks>Do not use the database object after calling DeleteDatabase().</remarks>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Scalien.Client.DeleteDatabase(string)"/>
        public void DeleteDatabase()
        {
            client.DeleteDatabase(this.Name);
        }
    }
}
