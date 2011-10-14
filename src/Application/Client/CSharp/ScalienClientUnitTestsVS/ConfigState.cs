using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ScalienClientUnitTesting
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
        // shards
    }

    public class ShardServer
    {
        public Int64 nodeID;
        public string endpoint;
        public bool hasHeartbeat;
        // quorum infos
        // quorum shard infos
    }

    public class ConfigState
    {
        public Int64 paxosID;
        public Int64 master;

        public List<Controller> controllers;
        public List<Quorum> quorums;
        // databases
        // tables
        // shards
        public List<ShardServer> shardServers;

        public ConfigState()
        {
        }
    }
}
