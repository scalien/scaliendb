using System;
using System.Collections.Generic;
using System.Text;

namespace Scalien
{
    /// <summary>
    /// Table is a convenience class for encapsulating table related operations.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ScalienDB uses databases and tables to manage key value namespaces.
    /// </para>
    /// </remarks>
    /// <example><code>
    /// db = client.GetDatabase("testDatabase");
    /// table = db.GetTable("testTable");
    /// // some sets
    /// using (client.Begin())
    /// {
    ///     for (i = 0; i &lt; 1000; i++) 
    ///         table.Set("foo" + i, "foo" + i);
    /// }
    /// using (client.Begin())
    /// {
    ///     for (i = 0; i &lt; 1000; i++) 
    ///         table.Set("bar" + i, "bar" + i);
    /// }
    /// // some deletes
    /// table.Delete("foo0");
    /// table.Delete("foo10");
    /// client.Submit();
    /// // count
    /// System.Console.WriteLine("number of keys starting with foo: " + table.Count(new StringRangeParams().Prefix("foo")));
    /// // iterate
    /// foreach(KeyValuePair&lt;string, string&gt; kv in table.GetKeyValueIterator(new StringRangeParams().Prefix("bar")))
    ///     System.Console.WriteLine(kv.Key + " => " + kv.Value);
    /// // truncate
    /// table.Truncate();
    /// </code></example>
    /// <seealso cref="Scalien.Database.CreateTable(string)"/>
    /// <seealso cref="Scalien.Database.CreateTable(string, Quorum)"/>
    /// <seealso cref="Scalien.Database.GetTable(string)"/>
    /// <seealso cref="Database"/>
    public class Table
    {
        private Client client;
        private Database database;
        private ulong tableID;
        private string name;

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
        /// The database this table is in.
        /// </summary>
        public Database Database
        {
            get
            {
                return database;
            }
        }

        /// <summary>
        /// The name of the table.
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

        internal Table(Client client, Database database, ulong tableID, string name)
        {
            this.client = client;
            this.database = database;
            this.tableID = tableID;
            this.name = name;
        }

        #endregion

        #region Table management

        /// <summary>
        /// Rename the table.
        /// </summary>
        /// <param name="newName">The new name of the table.</param>
        /// <exception cref="SDBPException"/>
        public void RenameTable(string newName)
        {
            int status = scaliendb_client.SDBP_RenameTable(client.cptr, tableID, newName);
            client.CheckResultStatus(status);
        }

        /// <summary>
        /// Delete the table.
        /// </summary>
        /// <exception cref="SDBPException"/>
        public void DeleteTable()
        {
            int status = scaliendb_client.SDBP_DeleteTable(client.cptr, tableID);
            client.CheckResultStatus(status);
        }

        /// <summary>
        /// Truncate the table. This means all key-values in the table are dropped.
        /// </summary>
        /// <exception cref="SDBPException"/>
        public void TruncateTable()
        {
            int status = scaliendb_client.SDBP_TruncateTable(client.cptr, tableID);
            client.CheckResultStatus(status);
        }

        #endregion

        #region Data commands

        /// <summary>
        /// Retrieve a value by key from the table. Returns null if not found.
        /// </summary>
        /// <param name="key">The key to look for.</param>
        /// <returns>The retrieved value.</returns>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Get(byte[])"/>
        public string Get(string key)
        {
            return client.Get(tableID, key);
        }

        /// <summary>
        /// Retrieve a value by key from the table. Returns null if not found.
        /// </summary>
        /// <param name="key">The key to look for.</param>
        /// <returns>The retrieved value.</returns>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Get(string)"/>
        public byte[] Get(byte[] key)
        {
            return client.Get(tableID, key);
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
            client.Set(tableID, key, value);
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
            client.Set(tableID, key, value);
        }

        /// <summary>
        /// Delete a key-value pair by key in the table.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Delete(byte[])"/>
        public void Delete(string key)
        {
            client.Delete(tableID, key);
        }

        /// <summary>
        /// Delete a key-value pair by key in the table.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Delete(string)"/>
        public void Delete(byte[] key)
        {
            client.Delete(tableID, key);
        }

        /// <summary>
        /// Return the number of matching keys in the table.
        /// </summary>
        /// <param name="ps">The filter parameters.</param>
        /// <returns>The number of matching keys in the table.</returns>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Scalien.StringRangeParams"/>
        /// <seealso cref="Count(ByteRangeParams)"/>
        public ulong Count(StringRangeParams ps)
        {
            return client.Count(tableID, ps);
        }

        /// <summary>
        /// Return the number of matching keys in the table.
        /// </summary>
        /// <param name="ps">The filter parameters.</param>
        /// <returns>The number of matching keys in the table.</returns>
        /// <exception cref="SDBPException"/>
        /// <seealso cref="Scalien.ByteRangeParams"/>
        /// <seealso cref="Count(StringRangeParams)"/>
        public ulong Count(ByteRangeParams ps)
        {
            return client.Count(tableID, ps);
        }

        #endregion

        #region Iterators

        /// <summary>
        /// Return an iterator that will return only keys.
        /// </summary>
        /// <param name="ps">The parameters of iteration, as a <see cref="Scalien.StringRangeParams"/>.</param>
        /// <returns>The iterator.</returns>
        /// <example><code>
        /// db = client.GetDatabase("testDatabase");
        /// table = db.GetTable("testTable");
        /// foreach (string s in table.GetKeyIterator(new StringRangeParams().Prefix("foo")))
        ///     System.Console.WriteLine(s);
        /// </code></example>
        /// <seealso cref="Scalien.StringRangeParams"/>
        /// <seealso cref="GetKeyIterator(ByteRangeParams)"/>
        /// <seealso cref="GetKeyValueIterator(StringRangeParams)"/>
        /// <seealso cref="GetKeyValueIterator(ByteRangeParams)"/>
        public StringKeyIterator GetKeyIterator(StringRangeParams ps)
        {
            return new StringKeyIterator(this, ps);
        }

        /// <summary>
        /// Return an iterator that will return only keys.
        /// </summary>
        /// <param name="ps">The parameters of iteration, as a <see cref="Scalien.ByteRangeParams"/>.</param>
        /// <returns>The iterator.</returns>
        /// <seealso cref="Scalien.ByteRangeParams"/>
        /// <seealso cref="GetKeyIterator(ByteRangeParams)"/>
        /// <seealso cref="GetKeyValueIterator(StringRangeParams)"/>
        /// <seealso cref="GetKeyValueIterator(ByteRangeParams)"/>
        public ByteKeyIterator GetKeyIterator(ByteRangeParams ps)
        {
            return new ByteKeyIterator(this, ps);
        }

        /// <summary>
        /// Return an iterator that will return keys and values as a <see cref="System.Collections.Generic.KeyValuePair{T, T}"/>.
        /// </summary>
        /// <param name="ps">The parameters of iteration, as a <see cref="Scalien.StringRangeParams"/>.</param>
        /// <returns>The iterator.</returns>
        /// <example><code>
        /// db = client.GetDatabase("testDatabase");
        /// table = db.GetTable("testTable");
        /// foreach (KeyValuePair&lt;string, string&gt; kv in table.GetKeyValueIterator(new StringRangeParams().Prefix("foo")))
        ///     System.Console.WriteLine(kv.Key + " => " + kv.Value);
        /// </code></example>
        /// <seealso cref="System.Collections.Generic.KeyValuePair{T, T}"/>
        /// <seealso cref="Scalien.StringRangeParams"/>
        /// <seealso cref="GetKeyValueIterator(ByteRangeParams)"/>
        /// <seealso cref="GetKeyIterator(StringRangeParams)"/>
        /// <seealso cref="GetKeyIterator(ByteRangeParams)"/>
        public StringKeyValueIterator GetKeyValueIterator(StringRangeParams ps)
        {
            return new StringKeyValueIterator(this, ps);
        }

        /// <summary>
        /// Return an iterator that will return keys and values as a <see cref="System.Collections.Generic.KeyValuePair{T, T}"/>.
        /// </summary>
        /// <param name="ps">The parameters of iteration, as a <see cref="Scalien.ByteRangeParams"/>.</param>
        /// <returns>The iterator.</returns>
        /// <seealso cref="System.Collections.Generic.KeyValuePair{T, T}"/>
        /// <seealso cref="Scalien.ByteRangeParams"/>
        /// <seealso cref="GetKeyValueIterator(StringRangeParams)"/>
        /// <seealso cref="GetKeyIterator(StringRangeParams)"/>
        /// <seealso cref="GetKeyIterator(ByteRangeParams)"/>
        public ByteKeyValueIterator GetKeyValueIterator(ByteRangeParams ps)
        {
            return new ByteKeyValueIterator(this, ps);
        }

        #endregion

        #region Retrieve helpers: Sequence, Submitter

        /// <summary>
        /// Retrieve a <see cref="Scalien.Sequence"/> backed by a key-value in this table.
        /// </summary>
        /// <param name="key">The key backing the sequence.</param>
        /// <returns>The corresponding <see cref="Scalien.Sequence"/> object.</returns>
        /// <seealso cref="GetSequence(byte[])"/>
        /// <seealso cref="Scalien.Sequence"/>
        public Sequence GetSequence(string key)
        {
            return new Sequence(this.client, this.tableID, key);
        }

        /// <summary>
        /// Retrieve a <see cref="Scalien.Sequence"/> backed by a key-value in this table.
        /// </summary>
        /// <param name="key">The key backing the sequence.</param>
        /// <returns>The corresponding <see cref="Scalien.Sequence"/> object.</returns>
        /// <seealso cref="GetSequence(string)"/>
        /// <seealso cref="Scalien.Sequence"/>
        public Sequence GetSequence(byte[] key)
        {
            return new Sequence(this.client, this.tableID, key);
        }

        #endregion
    }
}
