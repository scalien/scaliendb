using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Runtime.Serialization.Json;

using Scalien;

// TODO
// move Killing feature to it's own, independent class
// Kill controllers too
// configurable crash or sleep

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
        public int repeat;

        public KillerConf(int TimeOut, KillMode Mode, int Repeat)
        {
            timeout = TimeOut;
            mode = Mode;
            // victims
            repeat = Repeat;
        }
    }

    [TestClass]
    class FailOverTests
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
            Client client = new Client(Config.GetNodes());

            vix = 0;
            victim = null;

            while (actions.Count > 0)
            {
                Thread.Sleep(actions[0].timeout);

                cstate = Utils.JsonDeserialize<ConfigState>(System.Text.Encoding.UTF8.GetBytes(client.GetJSONConfigState()));
                if (cstate.quorums.Count < 1) Assert.Throw("No quorum in ConfigState");

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

                        foreach(ShardServer shardsrv in cstate.shardServers)
                        {
                            if (shardsrv.nodeID == vix)
                            {
                                victim = shardsrv.endpoint;
                                break;
                            }
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

                            foreach (ShardServer shardsrv in cstate.shardServers)
                            {
                                if (shardsrv.nodeID == vix)
                                {
                                    victim = shardsrv.endpoint;
                                    break;
                                }
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

                        foreach(ShardServer shardsrv in cstate.shardServers)
                        {
                            if (shardsrv.nodeID == vix)
                            {
                                victim = shardsrv.endpoint;
                                break;
                            }
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

                            foreach (ShardServer shardsrv in cstate.shardServers)
                            {
                                if (shardsrv.nodeID == vix)
                                {
                                    victim = shardsrv.endpoint;
                                    break;
                                }
                            }
                        }
                        break;
                }

                if (victim != null)
                {
                    // take sleep or crash action randomly
                    victim = victim.Substring(0, victim.Length - 4) + "8090";
                    // TODO event control
                    if (Utils.RandomNumber.Next(6) > 6)
                    {
                        // crash
                        url = "http://" + victim + "/debug?crash";
                        System.Console.WriteLine("Shard action(" + vix + "): " + url);
                    }
                    else
                    {
                        // sleep 20 seconds
                        url = "http://" + victim + "/debug?sleep=25";
                        System.Console.WriteLine("Shard action(" + vix + "): " + url);
                    }

                    System.Console.WriteLine(Utils.HTTP_GET(url, 3000));
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
            int loop = System.Convert.ToInt32(param);
            int users_per_iteration = 2;

            Users usr = new Users(Config.GetNodes());
            while (loop-- > 0)
            {
                usr.TestCycle(users_per_iteration);
            }
        }

        //[TestMethod]
        public void TestConfigState() // debug purposes only
        {
            Client client = new Client(Config.GetNodes());

            string config_string = client.GetJSONConfigState();
            System.Console.WriteLine(config_string);
            
            ConfigState conf = Utils.JsonDeserialize<ConfigState>(System.Text.Encoding.UTF8.GetBytes(config_string));

        }

        [TestMethod]
        public void TestRandomCrash()
        {
            Client.SetTrace(true);
            Client.SetLogFile("c:\\Users\\zszabo\\logs\\client_trace.txt");
            int init_users = 10000;
            int threadnum = 10;

            /*FileStream fs = new FileStream("c:\\Users\\zszabo\\logs\\threadout_10.txt", FileMode.Create);
            StreamWriter sw = new StreamWriter(fs);
            Console.SetOut(sw);*/

            Users usr = new Users(Config.GetNodes());
            usr.EmptyAll();
            usr.InsertUsers(init_users);

            Thread[] threads = new Thread[threadnum];
            for (int i = 0; i < threadnum; i++)
            {
                threads[i] = new Thread(new ParameterizedThreadStart(TestWorker));
                threads[i].Start(500);
            }

            Thread killer = new Thread(new ParameterizedThreadStart(Killer));
            killer.Start(new KillerConf(10000, KillMode.KILL_MAJORITY, 10));

            for (int i = 0; i < threadnum; i++)
            {
                threads[i].Join();
            }

            Assert.IsTrue(usr.IsConsistent());

            killer.Abort();
        }

        //[TestMethod]
        public void TestMasterCrash()
        {
        }

        /*
        [TestMethod]
        public void TestMasterCrash()
        {
        }*/
    }
}
