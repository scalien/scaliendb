using System;
using System.Collections.Generic;
using System.Text;

namespace Scalien
{
    /// <summary>
    /// Table is a convenience class for encapsulating table related operations.
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
    /// // some sets
    /// using (table.Begin())
    /// {
    ///     for (i = 0; i &lt; 1000; i++) 
    ///         table.Set("foo" + i, "foo" + i);
    /// }
    /// using (table.Begin())
    /// {
    ///     for (i = 0; i &lt; 1000; i++) 
    ///         table.Set("bar" + i, "bar" + i);
    /// }
    /// // some deletes
    /// table.Delete("foo0");
    /// table.Delete("foo10");
    /// // count
    /// System.Console.WriteLine("number of keys starting with foo: " + table.Count(new StringIterParams().Prefix("foo")));
    /// // iterate
    /// foreach(KeyValuePair&lt;string, string&gt; kv in table.KeyValueIterator(new StringIterParams().Prefix("bar")))
    ///     System.Console.WriteLine(kv.Key + " => " + kv.Value);
    /// // truncate
    /// table.Truncate();
    /// </code></example>
    /// 
    /// <seealso cref="Scalien.Client.CreateDatabase(string)"/>
    /// <seealso cref="Scalien.Client.GetDatabase(string)"/>
    /// <seealso cref="Table"/>
    /// <seealso cref="Quorum"/>
    public class Table
    {
        private Client client;
        private Database database;
        private string name;
        private ulong tableID;

        #region Properties

        internal Client Client
        {
            get
            {
                return client;
            }
        }

        internal ulong TableID
        {
            get
            {
                return tableID;
            }
        }

        /// <summary>
        /// Return the database this table is in.
        /// </summary>
        public Database Database
        {
            get
            {
                return database;
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

        internal Table(Client client, Database database, string name)
        {
            this.client = client;
            this.database = database;
            this.name = name;

            tableID = scaliendb_client.SDBP_GetTableID(client.cptr, database.DatabaseID, name);
            if (tableID == 0)
                throw new SDBPException(Status.SDBP_BADSCHEMA);
        }

        #endregion

        #region Internal and private helpers

        private void UseDefaults()
        {
            client.UseDatabaseID(database.DatabaseID);
            client.UseTableID(tableID);
        }

        internal long Add(string key, long value)
        {
            UseDefaults();
            return client.Add(key, value);
        }

        internal long Add(byte[] key, long value)
        {
            UseDefaults();
            return client.Add(key, value);
        }

        internal List<string> ListKeys(string startKey, string endKey, string prefix, uint count, bool skip)
        {
            UseDefaults();
            return client.ListKeys(startKey, endKey, prefix, count, skip);
        }

        internal List<byte[]> ListKeys(byte[] startKey, byte[] endKey, byte[] prefix, uint count, bool skip)
        {
            UseDefaults();
            return client.ListKeys(startKey, endKey, prefix, count, skip);
        }

        internal Dictionary<string, string> ListKeyValues(string startKey, string endKey, string prefix, uint count, bool skip)
        {
            UseDefaults();
            return client.ListKeyValues(startKey, endKey, prefix, count, skip);
        }

        internal Dictionary<byte[], byte[]> ListKeyValues(byte[] startKey, byte[] endKey, byte[] prefix, uint count, bool skip)
        {
            UseDefaults();
            return client.ListKeyValues(startKey, endKey, prefix, count, skip);
        }

        #endregion

        #region Table management

        /// <summary>
        /// Rename the table.
        /// </summary>
        /// <param name="newName">The new name of the table.</param>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Scalien.Client.RenameTable(string, string)"/>
        public void RenameTable(string newName)
        {
            UseDefaults();
            client.RenameTable(this.Name, newName);
        }

        /// <summary>
        /// Delete the table (all key-values in the table).
        /// </summary>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Scalien.Client.DeleteTable(string)"/>
        public void DeleteTable()
        {
            UseDefaults();
            client.DeleteTable(this.Name);
        }

        /// <summary>
        /// Truncate the table. This means all key-values in the table are dropped.
        /// </summary>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Scalien.Client.TruncateTable"/>
        public void TruncateTable()
        {
            UseDefaults();
            client.TruncateTable(this.Name);
        }

        #endregion

        #region Data commands

        /// <summary>
        /// Retrieve a value by key from the table. Throws exception if not found.
        /// </summary>
        /// <param name="key">The key to look for.</param>
        /// <returns>The retrieved value.</returns>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Get(byte[])"/>
        public string Get(string key)
        {
            UseDefaults();
            return client.Get(key);
        }

        /// <summary>
        /// Retrieve a value by key from the table. Throws exception if not found.
        /// </summary>
        /// <param name="key">The key to look for.</param>
        /// <returns>The retrieved value.</returns>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Get(string)"/>
        public byte[] Get(byte[] key)
        {
            UseDefaults();
            return client.Get(key);
        }

        /// <summary>
        /// Set a key-value in the table.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Set(byte[], byte[])"/>
        public void Set(string key, string value)
        {
            UseDefaults();
            client.Set(key, value);
        }

        /// <summary>
        /// Set a key-value in the table.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Set(string, string)"/>
        public void Set(byte[] key, byte[] value)
        {
            UseDefaults();
            client.Set(key, value);
        }

        /// <summary>
        /// Delete a key-value pair by key in the table.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Delete(byte[])"/>
        public void Delete(string key)
        {
            UseDefaults();
            client.Delete(key);
        }

        /// <summary>
        /// Delete a key-value pair by key in the table.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Delete(string)"/>
        public void Delete(byte[] key)
        {
            UseDefaults();
            client.Delete(key);
        }

        /// <summary>
        /// Return the number of matching keys in the table.
        /// </summary>
        /// <param name="ps">The parameters of iteration.</param>
        /// <returns>The number of matching keys in the table.</returns>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Scalien.StringIterParams"/>
        /// <seealso cref="Count(ByteIterParams)"/>
        public ulong Count(StringIterParams ps)
        {
            UseDefaults();
            return client.Count(ps);
        }

        /// <summary>
        /// Return the number of matching keys in the table.
        /// </summary>
        /// <param name="ps">The parameters of iteration.</param>
        /// <returns>The number of matching keys in the table.</returns>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Scalien.ByteIterParams"/>
        /// <seealso cref="Count(StringIterParams)"/>
        public ulong Count(ByteIterParams ps)
        {
            UseDefaults();
            return client.Count(ps);
        }

        #endregion

        #region Iterators

        /// <summary>
        /// Return an iterator that will return only keys.
        /// </summary>
        /// <param name="ps">The parameters of iteration, as a <see cref="Scalien.StringIterParams"/>.</param>
        /// <returns>The iterator.</returns>
        /// <example><code>
        /// db = client.GetDatabase("testDatabase");
        /// table = db.GetTable("testTable");
        /// foreach (string s in table.KeyIterator(new StringIterParams().Prefix("foo")))
        ///     System.Console.WriteLine(s);
        /// </code></example>
        /// <seealso cref="Scalien.StringIterParams"/>
        /// <seealso cref="KeyIterator(ByteIterParams)"/>
        /// <seealso cref="KeyValueIterator(StringIterParams)"/>
        /// <seealso cref="KeyValueIterator(ByteIterParams)"/>
        public StringKeyIterator KeyIterator(StringIterParams ps)
        {
            return new StringKeyIterator(this, ps);
        }

        /// <summary>
        /// Return an iterator that will return only keys.
        /// </summary>
        /// <param name="ps">The parameters of iteration, as a <see cref="Scalien.ByteIterParams"/>.</param>
        /// <returns>The iterator.</returns>
        /// <seealso cref="Scalien.ByteIterParams"/>
        /// <seealso cref="KeyIterator(ByteIterParams)"/>
        /// <seealso cref="KeyValueIterator(StringIterParams)"/>
        /// <seealso cref="KeyValueIterator(ByteIterParams)"/>
        public ByteKeyIterator KeyIterator(ByteIterParams ps)
        {
            return new ByteKeyIterator(this, ps);
        }

        /// <summary>
        /// Return an iterator that will return keys and values as a <see cref="System.Collections.Generic.KeyValuePair{T, T}"/>.
        /// </summary>
        /// <param name="ps">The parameters of iteration, as a <see cref="Scalien.StringIterParams"/>.</param>
        /// <returns>The iterator.</returns>
        /// <example><code>
        /// db = client.GetDatabase("testDatabase");
        /// table = db.GetTable("testTable");
        /// foreach (KeyValuePair&lt;string, string&gt; kv in table.KeyValueIterator(new StringIterParams().Prefix("foo")))
        ///     System.Console.WriteLine(kv.Key + " => " + kv.Value);
        /// </code></example>
        /// <seealso cref="System.Collections.Generic.KeyValuePair{T, T}"/>
        /// <seealso cref="Scalien.StringIterParams"/>
        /// <seealso cref="KeyValueIterator(ByteIterParams)"/>
        /// <seealso cref="KeyIterator(StringIterParams)"/>
        /// <seealso cref="KeyIterator(ByteIterParams)"/>
        public StringKeyValueIterator KeyValueIterator(StringIterParams ps)
        {
            return new StringKeyValueIterator(this, ps);
        }

        /// <summary>
        /// Return an iterator that will return keys and values as a <see cref="System.Collections.Generic.KeyValuePair{T, T}"/>.
        /// </summary>
        /// <param name="ps">The parameters of iteration, as a <see cref="Scalien.ByteIterParams"/>.</param>
        /// <returns>The iterator.</returns>
        /// <seealso cref="System.Collections.Generic.KeyValuePair{T, T}"/>
        /// <seealso cref="Scalien.ByteIterParams"/>
        /// <seealso cref="KeyValueIterator(StringIterParams)"/>
        /// <seealso cref="KeyIterator(StringIterParams)"/>
        /// <seealso cref="KeyIterator(ByteIterParams)"/>
        public ByteKeyValueIterator KeyValueIterator(ByteIterParams ps)
        {
            return new ByteKeyValueIterator(this, ps);
        }

        #endregion

        #region Retrieve helpers: Sequence, SubmitGuard

        /// <summary>
        /// Retrieve a <see cref="Scalien.Sequence"/> backed by a key-value in this table.
        /// </summary>
        /// <param name="key">The key backing the sequence.</param>
        /// <returns>The corresponding <see cref="Scalien.Sequence"/> object.</returns>
        /// <seealso cref="Scalien.Sequence"/>
        public Sequence GetSequence(string key)
        {
            return new Sequence(this.client, database.DatabaseID, this.tableID, key);
        }

        /// <summary>
        /// Retrieve a <see cref="Scalien.Sequence"/> backed by a key-value in this table.
        /// </summary>
        /// <param name="key">The key backing the sequence.</param>
        /// <returns>The corresponding <see cref="Scalien.Sequence"/> object.</returns>
        /// <seealso cref="Scalien.Sequence"/>
        public Sequence GetSequence(byte[] key)
        {
            return new Sequence(this.client, database.DatabaseID, this.tableID, key);
        }

        /// <summary>
        /// Return a new <see cref="Scalien.SubmitGuard"/> object.
        /// </summary>
        /// <returns>The <see cref="Scalien.SubmitGuard"/> object.</returns>
        /// <seealso cref="Scalien.SubmitGuard"/>
        public SubmitGuard Begin()
        {
            return new SubmitGuard(this.client);
        }

        #endregion

        #region Submit, Cancel

        /// <summary>
        /// Send all batched commands to the database server.
        /// </summary>
        /// <seealso cref="Scalien.Client.Submit()"/>
        public void Submit()
        {
            this.Client.Submit();
        }

        /// <summary>
        /// Discard all batched commands.
        /// </summary>
        /// <seealso cref="Scalien.Client.Cancel()"/>
        public void Cancel()
        {
            this.Client.Cancel();
        }

        #endregion
    }
}
