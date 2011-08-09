using System;
using System.Collections.Generic;

namespace Scalien
{
    /// <summary>
    /// Sequence is a class for atomically retrieving unique integer IDs
    /// using a key whose value stores the next ID as a piece of text.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ScalienDB is a key value store, so unique IDs have to be maintained
    /// in a separate key in a seperate table. For example:
    /// <para><c>people => 2500</c></para>
    /// In this case, the next sequence value returned would be 2500.
    /// </para>
    /// <para>
    /// ScalienDB has a special <c>ADD</c> command which parses the value as a number
    /// and increments it by a user specified value.
    /// The Index class wraps this functionality, and increments the number by a
    /// user-defined <c>granularity</c> (default 1000). This way, the server is contacted
    /// only every 1000th time the sequence is incremented, which is an important
    /// optimization since sending and waiting for commands to execute on the server
    /// is slow.
    /// </para>
    /// <para>
    /// Note that if the client quits before using up all the sequence values retrieved
    /// by the last <c>ADD</c> command, those will not be passed out, and there will
    /// be a hole in the sequence. 
    /// </para>
    /// <para>
    /// Use <see cref="Reset()"/> to reset the sequence to 0. This sets:
    /// </para>
    /// <para><c>
    /// key => 0
    /// </c></para>
    /// </remarks>
    /// <example><code>
    /// db = client.GetDatabase("testDatabase");
    /// indices = db.GetTable("indices");
    /// Sequence peopleIDs = indices.GetSequence("people");
    /// for (var i = 0; i &lt; 1000; i++)
    ///     System.Console.WriteLine(peopleIDs.GetNext);
    /// </code></example>
    /// <seealso cref="Table.GetSequence(string)"/>
    /// <seealso cref="Table.GetSequence(byte[])"/>
    public class Sequence
    {
        Client client;
        ulong databaseID;
        ulong tableID;
        string stringKey;
        byte[] byteKey;

        long granularity = 1000;
        long seq = 0;
        long num = 0;

        #region Constructors, destructors

        internal Sequence(Client client, ulong databaseID, ulong tableID, string key)
        {
            this.client = client;
            this.databaseID = databaseID;
            this.tableID = tableID;
            this.stringKey = key;
        }

        internal Sequence(Client client, ulong databaseID, ulong tableID, byte[] key)
        {
            this.client = client;
            this.databaseID = databaseID;
            this.tableID = tableID;
            this.byteKey = key;
        }

        #endregion

        #region Internal and private helpers

        private void AllocateRange()
        {
            ulong oldDatabaseID = client.GetDatabaseID();
            ulong oldTableID = client.GetTableID();

            if (oldDatabaseID != databaseID)
                client.UseDatabaseID(databaseID);
            if (oldTableID != tableID)
                client.UseTableID(tableID);

            if (stringKey != null)
                seq = client.Add(stringKey, granularity) - granularity;
            else
                seq = client.Add(byteKey, granularity) - granularity;
            num = granularity;

            if (oldDatabaseID != databaseID)
                client.UseDatabaseID(oldDatabaseID);
            if (oldTableID != tableID)
                client.UseTableID(oldTableID);
        }

        #endregion
        
        /// <summary>
        /// Set the granularity of the seqeunce increments (default 1000).
        /// </summary>
        /// <param name="granularity">The granularity of the sequence.</param>
        public void SetGranularity(long granularity)
        {
            this.granularity = granularity;
        }

        /// <summary>
        /// Reset the sequence to 0. The next time <see cref="GetNext"/> is called, 0 will be returned.
        /// </summary>
        /// <remarks>
        /// This sets:
        /// <para><c>
        /// key => 0
        /// </c></para>
        /// </remarks>
        public void Reset()
        {
            ulong oldDatabaseID = client.GetDatabaseID();
            ulong oldTableID = client.GetTableID();

            if (oldDatabaseID != databaseID)
                client.UseDatabaseID(databaseID);
            if (oldTableID != tableID)
                client.UseTableID(tableID);

            if (stringKey != null)
                client.Set(stringKey, "0");
            else
                client.Set(byteKey, Client.StringToByteArray("0"));
            num = 0;

            if (oldDatabaseID != databaseID)
                client.UseDatabaseID(oldDatabaseID);
            if (oldTableID != tableID)
                client.UseTableID(oldTableID);
        }

        /// <summary>
        /// Get the next sequence value.
        /// </summary>
        public long GetNext
        {
            get
            {
                if (num == 0)
                    AllocateRange();
                
                num--;
                return seq++;
            }
        }
    }
}
