using System;
using System.Collections.Generic;
using System.Text;

namespace Scalien
{
    /// <summary>
    /// Quorum is a convenience class for working wit quorums.
    /// </summary>
    /// <example><code>
    /// Quorum quorum = client.GetQuorum("quorum 1");
    /// // create table located in quorum
    /// Database db = client.GetDatabase("testDatabase");
    /// db.CreateTable("testTable", quorum)
    /// // list all quorums
    /// foreach (Quorum q in client.GetQuorums())
    ///     System.Console.WriteLine(q.Name);
    /// </code></example>
    /// <seealso cref="Client.GetQuorum(string)"/>
    /// <seealso cref="Client.GetQuorums()"/>
    /// <seealso cref="Database.CreateTable(string, Quorum)"/>
    public class Quorum
    {
        private Client client;
        private ulong quorumID;
        private string name;

        public ulong QuorumID
        {
            get
            {
                return quorumID;
            }
        }

        /// <summary>
        /// The name of the quorum.
        /// </summary>
        public virtual string Name
        {
            get
            {
                return name;
            }
        }

        internal Quorum(Client client, ulong quorumID, string name)
        {
            this.client = client;
            this.quorumID = quorumID;
            this.name = name;
        }
    }
}
