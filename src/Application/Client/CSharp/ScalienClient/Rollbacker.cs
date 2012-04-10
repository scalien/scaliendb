using System;

namespace Scalien
{
    /// <summary>
    /// Rollbacker is a convenience class returned by <see cref="Client.Start()"/>
    /// to ensure that <see cref="Client.RollbackTransaction()"/> is called when the Rollbacker goes
    /// out of scope.
    /// </summary>
    /// <example><code>
    /// using (client.Transaction(quorum, majorKey)) // returns a Rollbacker guard object
    /// {
    ///     table.Set("foo1", "bar1");
    ///     table.Set("foo2", "bar2");
    ///     client.CommitTransaction()
    /// }
    /// // the Rollbacker calls client.RollbackTransaction() when it goes out of scope,
    /// // which is a NOP if the client.CommitTransaction() on the previous line was reached
    /// </code></example>
    /// <seealso cref="Client.StartTransaction()"/>
    /// <seealso cref="Client.CommitTransaction()"/>
    /// <seealso cref="Client.RollbackTransaction()"/>
    public class Rollbacker : IDisposable
    {
        Client client;

        internal Rollbacker(Client client, Quorum quorum, byte[] majorKey)
        {
            this.client = client;

            client.StartTransaction(quorum, majorKey);
        }

        /// <summary>
        /// Commit transaction
        /// </summary>
        public void Dispose()
        {
            client.RollbackTransaction();
        }
    }
}
