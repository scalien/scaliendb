using System;

namespace Scalien
{
    /// <summary>
    /// SubmitGuard is a conveniance class returned by <see cref="Client.Begin()"/>
    /// to ensure that <see cref="Client.Submit()"/> is called when the SubmitGuard goes
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
    /// In cases like this, use a SubmitGuard to make sure that <see cref="Client.Submit()"/>
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
    /// <seealso cref="Client.Rollback()"/>
    public class SubmitGuard : IDisposable
    {
        Client client;
        bool cancelled = false;

        internal SubmitGuard(Client client)
        {
            this.client = client;
        }

        /// <summary>
        /// Submit will not be called when the object goes out of scope. 
        /// </summary>
        public void Cancel()
        {
            cancelled = true;
        }

        public void Dispose()
        {
            if (!cancelled)
                client.Submit();
        }
    }
}
