using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

#if !SCALIEN_UNIT_TEST_FRAMEWORK
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

using Scalien;

namespace ScalienClientUnitTesting
{
    //[TestClass]
    public class MultiClusterTests
    {
        private static void MultiClusterTestWorker(Object param)
        {
            int loop = System.Convert.ToInt32(param);

            Users usr = new Users(Utils.GetConfigNodes());
            while (loop-- > 0)
            {
                usr.TestCycle_MultiUser();
            }
        }

        //[TestMethod]
        public void MultiClusterTest_10_Threads()
        {
            int init_users = 10000;
            int threadnum = 10;

            Users usr = new Users(Utils.GetConfigNodes());
            // TODO add another client
            usr.EmptyAll();
            usr.InsertUsers(init_users);

            Thread[] threads = new Thread[threadnum];
            for (int i = 0; i < threadnum; i++)
            {
                threads[i] = new Thread(new ParameterizedThreadStart(MultiClusterTestWorker));
                threads[i].Start(500);
            }

            for (int i = 0; i < threadnum; i++)
            {
                threads[i].Join();
            }

            while(usr.IterateClients())
                Assert.IsTrue(usr.IsConsistent());

            Assert.IsTrue(usr.ClientDBsAreSame(0, 1));
        }

        public void MultiClusterTest_100_Threads()
        {
        }

        public void MultiClusterTest_1000_Threads()
        {
        }
    }
}
