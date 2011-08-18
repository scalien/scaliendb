using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Scalien
{
    /// <summary>
    /// ClientPool is a static class that manages a number of Client
    /// connections and caches them when not in use.
    /// <example><code>
    /// string[] controllers = { "localhost:7080" }
    /// ClientPool.Initialize(controllers, 16);
    /// using(PooledClient client = ClientPool.Acquire())
    /// {
    ///     Database db = client.GetDatabase("test");
    ///     Table table = db.GetTable("test");
    ///     table.Set("foo", "bar");
    ///     var value = table.Get("foo");
    /// }   // client.submit() is automatically called at the end of the using block
    /// </code></example>
    /// </summary>
    public class ClientPool
    {
        /// <summary>
        /// ClientPool returns PooledClient objects, which are like regular Clients.
        /// </summary>
        /// <seealso cref="Scalien.Client"/>
        public class PooledClient : Client, IDisposable
        {
            internal PooledClient(string[] controllers)
            :
                base(controllers)
            {
            }

            /// <summary>
            /// Send batched commands to the server and put Client back into the pool.
            /// </summary>
            public void Dispose()
            {
                Submit();
                ClientPool.Release(this);
            }
        }

        private static string[] controllers;
        private static List<PooledClient> clients = new List<PooledClient>();
        private static uint poolSize = 0;

        /// <summary>
        /// Initialize the ClientPool. Must be called exactly once at the beginning of the program.
        /// </summary>
        /// <param name="controllers">The controllers.</param>
        /// <param name="poolSize">The size of the pool, eg. 16.</param>
        public static void Initialize(string[] controllers, uint poolSize)
        {
            lock (clients)
            {
                ClientPool.poolSize = poolSize;
                ClientPool.controllers = controllers;

                while (clients.Count > poolSize)
                {
                    clients.RemoveAt(0);
                }

                while (clients.Count < poolSize)
                {
                    clients.Add(new PooledClient(controllers));
                }
            }
        }

        /// <summary>
        /// Return the controllers connection string array.
        /// </summary>
        public static string[] Controllers
        {
            get
            {
                return controllers;
            }
        }

        /// <summary>
        /// Return the pool size.
        /// </summary>
        public static uint PoolSize
        {
            get
            {
                return poolSize;
            }
        }

        /// <summary>
        /// Acquire a new Client object from the pool.
        /// </summary>
        /// <returns></returns>
        public static PooledClient Acquire()
        {
            if (poolSize < 1)
                throw new SDBPException(Status.SDBP_API_ERROR, "Uninitialized static class");

            PooledClient client;

            lock (clients)
            {
                if (clients.Count > 0)
                {
                    client = clients[0];
                    clients.RemoveAt(0);
                }
                else
                {
                    client = new PooledClient(controllers);
                }
            }
            return client;
        }

        internal static void Release(PooledClient client)
        {
            if (poolSize < 1)
                throw new SDBPException(Status.SDBP_API_ERROR, "Uninitialized static class");

            lock (clients)
            {
                if (clients.Count < poolSize)
                    clients.Add(client);
            }
        }
    }
}
