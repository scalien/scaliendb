using System;

namespace Scalien
{
    /// <summary>
    /// SubmitGuard is a conveniance class returned by <see cref="Table.Begin()"/>
    /// to ensure that <see cref="Table.Submit()"/> is called when the SubmitGuard goes
    /// out of scope.
    /// </summary>
    /// <remarks>
    /// <para>
    /// By default the ScalienDB client automatically collects commands
    /// and sends them off automatically in batches for improved efficiency.
    /// Sometimes application logic requires that <see cref="Table.Submit()"/>
    /// be called explicitly to make sure the commands sent and executed on the server.
    /// </para>
    /// <para>
    /// In cases like this, use a SubmitGuard to make sure that <see cref="Table.Submit()"/>
    /// is automatically called when it goes out of scope.
    /// </para>
    /// </remarks>
    /// <example><code>
    /// using (client.Begin())
    /// {
    ///     client.Set("foo1", "bar1");
    ///     client.Set("foo2", "bar2");
    /// }
    /// // client.Submit() is called automatically here
    /// </code></example>
    /// <seealso cref="Table.Begin()"/>
    /// <seealso cref="Table.Submit()"/>
    public class SubmitGuard : IDisposable
    {
        Client client;

        internal SubmitGuard(Client client)
        {
            this.client = client;
        }

        public void Dispose()
        {
            client.Submit();
        }
    }
}
