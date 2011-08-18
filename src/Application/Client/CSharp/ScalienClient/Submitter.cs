using System;

namespace Scalien
{
    /// <summary>
    /// Submitter is a convenience class returned by <see cref="Client.Begin()"/>
    /// to ensure that <see cref="Client.Submit()"/> is called when the Submitter goes
    /// out of scope.
    /// </summary>
    /// <remarks>
    /// <para>
    /// By default the ScalienDB client automatically collects commands
    /// and sends them off automatically in batches for improved efficiency.
    /// Sometimes application logic requires that <see cref="Client.Submit()"/>
    /// be called explicitly to make sure the commands sent and executed on the server.
    /// </para>
    /// <para>
    /// In cases like this, use a Submitter to make sure that <see cref="Client.Submit()"/>
    /// is automatically called when it goes out of scope.
    /// </para>
    /// </remarks>
    /// <example><code>
    /// using (client.Begin())
    /// {
    ///     table.Set("foo1", "bar1");
    ///     table.Set("foo2", "bar2");
    /// }
    /// // client.Submit() is called automatically here
    /// </code></example>
    /// <seealso cref="Client.Begin()"/>
    /// <seealso cref="Client.Submit()"/>
    public class Submitter : IDisposable
    {
        Client client;

        internal Submitter(Client client)
        {
            this.client = client;
        }

        /// <summary>
        /// Send batched commands to the server.
        /// </summary>
        public void Dispose()
        {
            client.Submit();
        }
    }
}
