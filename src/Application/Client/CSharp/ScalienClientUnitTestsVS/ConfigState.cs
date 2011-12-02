using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ScalienClientUnitTesting
{
    public class ConfigState
    {
        public class Controller
        {
            public Int64 nodeID;
            public string endpoint;
            public bool isConnected;
        }

        public class Quorum
        {
            public Int64 quorumID;
            public string name;
            public bool hasPrimary;
            public Int64 primaryID;
            public Int64 paxosID;

            public List<Int64> activeNodes;
            public List<Int64> inactiveNodes;
            public List<Int64> shards;
        }

        public class Shard
        {
            public Int64 quorumID;
            public Int64 tableID;
            public Int64 shardID;
            public uint state;
            public Int64 parentShardID;
            public Int64 shardSize;
            public string splitKey;
            public bool isSplitable;
            public string firstKey;
            public string lastKey;
        }

        public class QuorumShardInfo
        {
            public Int64 shardID;
            public bool isSendingShard;
            public Int64 migrationQuorumID;
            public Int64 migrationNodeID;
            public Int64 migrationBytesSent;
            public Int64 migrationBytesTotal;
            public Int64 migrationThroughput;
        }

        public class ShardServer
        {
            public Int64 nodeID;
            public string endpoint;
            public bool hasHeartbeat;
            public uint httpPort;
            public uint sdbpPort;
            // quorum infos
            public List<QuorumShardInfo> quorumShardInfos;
        }

        public class Table
        {
            public Int64 databaseID;
            public Int64 tableID;
            public string name;
            public bool isFrozen;
            public List<Int64> shards;
        }

        public class Database
        {
            public Int64 databaseID;
            public string name;
            public List<Int64> tables;
        }

        public Int64 paxosID;
        public Int64 master;

        public List<Controller> controllers;
        public List<Quorum> quorums;
        public List<Database> databases;
        public List<Table> tables;
        public List<Shard> shards;
        public List<ShardServer> shardServers;

        public ConfigState()
        {
        }


    }
}
