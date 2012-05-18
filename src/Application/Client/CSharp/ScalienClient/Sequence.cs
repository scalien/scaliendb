using System;
using System.Collections.Generic;

namespace Scalien
{
    /// <summary>
    /// Sequence is a class for atomically retrieving unique integer IDs
    /// using a key whose value stores the next ID as a piece of text.
    /// The first returned ID is 1.
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
    /// user-defined <c>gran</c> (default 1000). This way, the server is contacted
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
    /// Use <see cref="Reset()"/> to reset the sequence to 1. This sets:
    /// </para>
    /// <para><c>
    /// key => 1
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
        ulong tableID;
        byte[] key;

        #region Constructors, destructors

        internal Sequence(Client client, ulong tableID, string key)
        {
            this.client = client;
            this.tableID = tableID;
            this.key = Client.StringToByteArray(key);
        }

        internal Sequence(Client client, ulong tableID, byte[] key)
        {
            this.client = client;
            this.tableID = tableID;
            this.key = key;
        }

        #endregion
      
        /// <summary>
        /// Set the gran of the sequence increments (default 1000).
        /// </summary>
        /// <param name="gran">The gran of the sequence.</param>
        [System.Obsolete]
        public virtual void SetGranularity(long gran)
        {
        }

        /// <summary>
        /// Reset the sequence to 1. The next time <see cref="GetNext"/> is called, 1 will be returned.
        /// </summary>
        /// <remarks>
        /// This sets:
        /// <para><c>
        /// key => 1
        /// </c></para>
        /// </remarks>
        public virtual void Reset()
        {
            client.SequenceSet(tableID, key, 1);
        }

        /// <summary>
        /// Set the sequence to a given value. The next time <see cref="GetNext"/> is called, value will be returned.
        /// </summary>
        /// <remarks>
        /// This sets:
        /// <para><c>
        /// key => value
        /// </c></para>
        /// </remarks>
        public virtual void Set(ulong value)
        {
            client.SequenceSet(tableID, key, value);
        }

        /// <summary>
        /// Get the next sequence value.
        /// </summary>
        public virtual ulong GetNext
        {
            get
            {
                return client.SequenceNext(tableID, key);
            }
        }
    }
}
