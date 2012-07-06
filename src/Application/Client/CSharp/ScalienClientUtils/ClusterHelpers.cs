using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Scalien
{
    public class ClusterHelpers
    {
        public static bool RandomShardServerHTTPAction(Client client, Quorum quorum, string action)
        {
            var configState = Utils.GetFullConfigState(client);
            var shardServers = configState.shardServers;
            var configQuorum = configState.quorums.First(cq => cq.quorumID == (long)quorum.QuorumID);
            var random = new Random();

            // select shard server
            var victimNodeID = configQuorum.activeNodes[random.Next(configQuorum.activeNodes.Count)];
            foreach (var shardServer in shardServers)
            {
                if (shardServer.nodeID == victimNodeID)
                {
                    var httpURI = ConfigStateHelpers.GetShardServerURL(shardServer);
                    var response = Utils.HTTP.GET(Utils.HTTP.BuildUri(httpURI, action));
                    if (response == null)
                        return false;
                    return true;
                }
            }
            return false;
        }

        public static bool PrimaryShardServerHTTPAction(Client client, Quorum quorum, string action)
        {
            var configState = Utils.GetFullConfigState(client);
            var shardServers = configState.shardServers;
            var configQuorum = configState.quorums.First(cq => cq.quorumID == (long)quorum.QuorumID);
            if (configQuorum.hasPrimary == false)
                return false;

            var shardServer = configState.shardServers.First(s => s.nodeID == configQuorum.primaryID);
            var httpURI = ConfigStateHelpers.GetShardServerURL(shardServer);
            var response = Utils.HTTP.GET(Utils.HTTP.BuildUri(httpURI, action));
            if (response == null)
                return false;

            return true;
        }

        public static string SleepAction(string debugKey, int sleepInterval)
        {
            return new Utils.HTTP.QueryBuilder("debug").Add("key", debugKey).Add("sleep", "" + sleepInterval).Query;
        }

    }
}
