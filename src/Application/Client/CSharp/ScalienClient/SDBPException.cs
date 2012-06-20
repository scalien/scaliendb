using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Scalien
{
    /// <summary>
    /// ScalienDB exceptions.
    /// </summary>
    public class SDBPException : Exception
    {
        private int status;
        internal UInt64? nodeID;
        internal UInt64? quorumID;
        internal UInt64? tableID;
        internal UInt64? paxosID;

        /// <summary>
        /// The status code of the exception.
        /// </summary>
        public int Status
        {
            get
            {
                return status;
            }
        }

        /// <summary>
        /// The nodeID of the server where the last command was sent
        /// </summary>
        public UInt64? NodeID
        {
            get
            {
                return nodeID;
            }
        }

        /// <summary>
        /// The quorumID of the command that generated the exception
        /// </summary>
        public UInt64? QuorumID
        {
            get
            {
                return quorumID;
            }
        }

        /// <summary>
        /// The tableID of the command that generated the exception
        /// </summary>
        public UInt64? TableID
        {
            get
            {
                return tableID;
            }
        }

        /// <summary>
        /// The paxosID of the command that generated the exception
        /// </summary>
        public UInt64? PaxosID
        {
            get
            {
                return paxosID;
            }
        }

        internal SDBPException(int status)
            : base(Scalien.Status.ToString(status) + " (code " + status + ")")
        {
            Init(status);
        }

        internal SDBPException(int status, string txt)
            : base(Scalien.Status.ToString(status) + " (code " + status + "): " + txt)
        {
            Init(status);
        }

        private void Init(int status)
        {
            this.status = status;
            nodeID = null;
            quorumID = null;
            tableID = null;
            paxosID = null;
        }
    }
}
