using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Runtime.Serialization.Json;

#if !SCALIEN_UNIT_TEST_FRAMEWORK
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

using Scalien;

// TODO
// move Killing feature to it's own, independent class
// Kill controllers too
// configurable crash or sleep
// make several tests

// http://192.168.137.103:38080/debug?crash
// http://192.168.137.103:38080/debug?sleep=10 seconds
namespace ScalienClientUnitTesting
{
    enum KillMode
    {
        KILL_ONE_RANDOMLY,
        KILL_ONE_PRIMARY,
        KILL_MAJORITY,
        KILL_REPETITIVELY
    };

    enum KillVictimType
    {
        KILL_CONTROLLERS,
        KILL_SHARDS,
        KILL_RANDOMLY_BOTH
    };

    enum KillActionType
    {
        KILL_USING_CRASH,
        KILL_USING_SLEEP,
        KILL_USING_BOTH_RANDOMLY
    }
    
    /*
     * KillerConf
     * TimeOut - milliseconds before action
     * Mode    - action policy
     * Repeat  - number of repeats (-1 for infinite)
     * */
    class KillerConf
    {
        public int timeout;
        public KillMode mode;
        public KillVictimType victimtype;
        public KillActionType action;
        public int repeat;

        public KillerConf(int TimeOut, KillMode Mode, int Repeat, KillActionType Action = KillActionType.KILL_USING_BOTH_RANDOMLY, KillVictimType VictimType = KillVictimType.KILL_RANDOMLY_BOTH)
        {
            timeout = TimeOut;
            mode = Mode;
            repeat = Repeat;

            action = Action;
            victimtype = VictimType;
        }
    }

    [TestClass]
    public class FailOverTests
    {
        public void Killer(Object param)
        {
            string victim;
            Int64 vix;
            string url;
            ConfigState cstate;

            List<KillerConf> actions;
            if (param is KillerConf)
            {
                actions = new List<KillerConf>();
                actions.Add((KillerConf)param);
            }
            else
            {
                actions = (List<KillerConf>)param;
            }
            Client client = new Client(Utils.GetConfigNodes());

            vix = 0;
            victim = null;

            while (actions.Count > 0)
            {
                Thread.Sleep(actions[0].timeout);

                cstate = Utils.JsonDeserialize<ConfigState>(System.Text.Encoding.UTF8.GetBytes(client.GetJSONConfigState()));
                if (cstate.quorums.Count < 1) Assert.Fail("No quorum in ConfigState");

                // select victim and next timeout
                switch (actions[0].mode)
                {
                    case KillMode.KILL_ONE_RANDOMLY:
                        if (cstate.quorums[0].inactiveNodes.Count > 0)
                        {
                            System.Console.WriteLine("Cluster pardoned");
                            continue; // this mode kills only one
                        }

                        vix = cstate.quorums[0].activeNodes[Utils.RandomNumber.Next(cstate.quorums[0].activeNodes.Count)];

                        foreach (ConfigState.ShardServer shardsrv in cstate.shardServers)
                            if (shardsrv.nodeID == vix)
                            {
                                victim = shardsrv.endpoint;
                                break;
                            }

                        break;

                    case KillMode.KILL_ONE_PRIMARY:
                        if (cstate.quorums[0].inactiveNodes.Count > 0)
                        {
                            System.Console.WriteLine("Cluster pardoned");
                            continue; // this mode kills only one
                        }

                        if (cstate.quorums[0].hasPrimary)
                        {
                            vix = cstate.quorums[0].primaryID;

                            foreach (ConfigState.ShardServer shardsrv in cstate.shardServers)
                                if (shardsrv.nodeID == vix)
                                {
                                    victim = shardsrv.endpoint;
                                    break;
                                }
                        }
                        break;

                    case KillMode.KILL_MAJORITY:
                        if (cstate.quorums[0].activeNodes.Count < 2)
                        {
                            System.Console.WriteLine("Cluster pardoned");
                            continue; // keep one alive
                        }

                        vix = cstate.quorums[0].activeNodes[Utils.RandomNumber.Next(cstate.quorums[0].activeNodes.Count)];

                        foreach (ConfigState.ShardServer shardsrv in cstate.shardServers)
                            if (shardsrv.nodeID == vix)
                            {
                                victim = shardsrv.endpoint;
                                break;
                            }

                        break;

                    case KillMode.KILL_REPETITIVELY:
                        if (cstate.quorums[0].inactiveNodes.Count > 0)
                        {
                            System.Console.WriteLine("Cluster pardoned");
                            continue; // this mode kills only one
                        }

                        if (victim == null)
                        {
                            // choose the only victim and kill always him
                            vix = cstate.quorums[0].activeNodes[Utils.RandomNumber.Next(cstate.quorums[0].activeNodes.Count)];

                            foreach (ConfigState.ShardServer shardsrv in cstate.shardServers)
                                if (shardsrv.nodeID == vix)
                                {
                                    victim = shardsrv.endpoint;
                                    break;
                                }
                        }
                        break;
                }

                if (victim != null)
                {
                    string action_string = "";

                    switch (actions[0].action)
                    {
                        case KillActionType.KILL_USING_SLEEP:
                            action_string = "/debug?sleep=25";
                            break;
                        case KillActionType.KILL_USING_CRASH:
                            action_string = "/debug?crash";
                            break;
                        case KillActionType.KILL_USING_BOTH_RANDOMLY:
                            if (Utils.RandomNumber.Next(10) < 5)
                                action_string = "/debug?sleep=25";
                            else
                                action_string = "/debug?crash";
                            break;
                    }

                    victim = victim.Substring(0, victim.Length - 4) + "8090";
                    url = "http://" + victim + action_string;
                    System.Console.WriteLine("Shard action(" + vix + "): " + url);

                    System.Console.WriteLine(Utils.HTTP.GET(url, 3000));
                }

                if (actions[0].repeat == 0) 
                {
                    actions.Remove(actions[0]); // remove action
                    victim = null;
                    continue;
                }
                
                if (actions[0].repeat > 0) actions[0].repeat--;
            }
        }

        private static void TestWorker(Object param)
        {
            Utils.TestThreadConf conf = (Utils.TestThreadConf)param;

            int loop = System.Convert.ToInt32(conf.param);
            int users_per_iteration = 2;

            try
            {
                Users usr = new Users(Utils.GetConfigNodes());
                while (loop-- > 0)
                {
                    usr.TestCycle(users_per_iteration);
                }
            }
            catch (Exception e)
            {
                lock (conf.exceptionsCatched)
                {
                    conf.exceptionsCatched.Add(e);
                }
            }
        }

        [TestMethod]
        public void TestConfigState() // debug purposes only
        {
            Client client = new Client(Utils.GetConfigNodes());

            client.SetGlobalTimeout(15000);

            string config_string = client.GetJSONConfigState();
            System.Console.WriteLine(config_string);
            
            ConfigState conf = Utils.JsonDeserialize<ConfigState>(System.Text.Encoding.UTF8.GetBytes(config_string));

        }

        [TestMethod]
        public void TestRandomCrash()
        {
            int init_users = 10000;
            int threadnum = 10;

            var nodes = Utils.GetConfigNodes();
            Users usr = new Users(Utils.GetConfigNodes());
            usr.EmptyAll();
            usr.InsertUsers(init_users);

            Utils.TestThreadConf threadConf = new Utils.TestThreadConf();
            threadConf.param = 500;

            Thread[] threads = new Thread[threadnum];
            for (int i = 0; i < threadnum; i++)
            {
                threads[i] = new Thread(new ParameterizedThreadStart(TestWorker));
                threads[i].Start(threadConf);
            }

            Thread killer = new Thread(new ParameterizedThreadStart(Killer));
            killer.Start(new KillerConf(10000, KillMode.KILL_MAJORITY, 10, KillActionType.KILL_USING_CRASH));

            for (int i = 0; i < threadnum; i++)
            {
                threads[i].Join();
            }

            if (threadConf.exceptionsCatched.Count > 0)
                Assert.Fail("Exceptions catched in threads", threadConf);

            Assert.IsTrue(usr.IsConsistent());

            killer.Abort();
        }

        [TestMethod]
        public void TestRandomSleep()
        {
            int init_users = 10000;
            int threadnum = 10;

            var nodes = Utils.GetConfigNodes();
            Users usr = new Users(Utils.GetConfigNodes());
            usr.EmptyAll();
            usr.InsertUsers(init_users);

            Utils.TestThreadConf threadConf = new Utils.TestThreadConf();
            threadConf.param = 500;

            Thread[] threads = new Thread[threadnum];
            for (int i = 0; i < threadnum; i++)
            {
                threads[i] = new Thread(new ParameterizedThreadStart(TestWorker));
                threads[i].Start(threadConf);
            }

            Thread killer = new Thread(new ParameterizedThreadStart(Killer));
            killer.Start(new KillerConf(10000, KillMode.KILL_MAJORITY, 10, KillActionType.KILL_USING_SLEEP));

            for (int i = 0; i < threadnum; i++)
            {
                threads[i].Join();
            }

            if (threadConf.exceptionsCatched.Count > 0)
                Assert.Fail("Exceptions catched in threads", threadConf);

            Assert.IsTrue(usr.IsConsistent());

            killer.Abort();
        }

        [TestMethod]
        public void TestRandomCrashShardServer()
        {
            var client = new Client(Utils.GetConfigNodes());

            Random random = new Random();

            while (true)
            {
                var jsonConfigState = client.GetJSONConfigState();
                var configState = Utils.JsonDeserialize<ConfigState>(System.Text.Encoding.UTF8.GetBytes(jsonConfigState));
                var shardServers = configState.shardServers;
                var quorum = configState.quorums[0];
                var numShardServers = shardServers.Count;

                if (quorum.inactiveNodes.Count > 0)
                {
                    var sleepTime = random.Next(10, 10 + random.Next(30));
                    Console.WriteLine("Inactive found, sleeping {0}...", sleepTime);
                    Thread.Sleep(sleepTime * 1000);
                    continue;
                }

                var victimNodeID = quorum.activeNodes[random.Next(quorum.activeNodes.Count)];
                foreach (var shardServer in shardServers)
                {
                    if (shardServer.nodeID == victimNodeID)
                    {
                        var shardHttpURI = ConfigStateHelpers.GetShardServerURL(shardServer);
                        Console.WriteLine("Killing {0}", shardHttpURI);
                        Utils.HTTP.GET(Utils.HTTP.BuildUri(shardHttpURI, "settings?assert"));
                        var sleepTime = random.Next(600, 600 + random.Next(300));
                        Console.WriteLine("Sleeping {0}...", sleepTime);
                        Thread.Sleep(sleepTime * 1000);
                    }
                }
                
            }
        }
    }
}
