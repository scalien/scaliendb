using System;
using System.Collections.Generic;
using System.Text;

namespace Scalien
{
    public class Quorum
    {
        private Client client;
        private ulong quorumID;

        public Quorum(Client client, ulong quorumID)
        {
            this.client = client;
            this.quorumID = quorumID;
        }

        public ulong GetQuorumID()
        {
            return quorumID;
        }

        public void DeleteQuorum()
        {
            client.DeleteQuorum(quorumID);
        }

        public void AddNode(ulong nodeID)
        {
            client.AddNode(quorumID, nodeID);
        }

        public void RemoveNode(ulong nodeID)
        {
            client.RemoveNode(quorumID, nodeID);
        }
    }
}
