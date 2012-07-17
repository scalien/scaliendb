using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;

#if !SCALIEN_UNIT_TEST_FRAMEWORK
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

using Scalien;
using System.Threading;
using System.Threading.Tasks;

namespace ScalienClientUnitTesting
{
    /// <summary>
    /// Summary description for TransactionTests
    /// </summary>
    [TestClass]
    public class TransactionTests
    {
        public static string quorumName = "Storage";
        public static string databaseName = "test_db";
        public static string tableName = "test_table";
        public static int lockTimeout = 10 * 1000;
        public static string debugKey = "N0226tpF27HnXqP";
        public static int sleepInterval = 10;

        public TransactionTests()
        {
            //
            // TODO: Add constructor logic here
            //
        }


        #region Additional test attributes
        //
        // You can use the following additional attributes as you write your tests:
        //
        // Use ClassInitialize to run code before running the first test in the class
        // [ClassInitialize()]
        // public static void MyClassInitialize(TestContext testContext) { }
        //
        // Use ClassCleanup to run code after all tests in a class have run
        // [ClassCleanup()]
        // public static void MyClassCleanup() { }
        //
        // Use TestInitialize to run code before running each test 
        // [TestInitialize()]
        // public void MyTestInitialize() { }
        //
        // Use TestCleanup to run code after each test has run
        // [TestCleanup()]
        // public void MyTestCleanup() { }
        //
        #endregion

        static int testAddNotExistingCounter;
        [TestMethod]
        public void TestAddNotExisting()
        {
            var client = Utils.GetClient();
            var quorum = client.GetQuorum(quorumName);
            var database = client.GetDatabase(databaseName);
            var table = database.GetTable(tableName);
            var counter = Interlocked.Increment(ref testAddNotExistingCounter);
            byte[] majorKey = Utils.StringToByteArray("TestAddNotExisting" + counter);
            byte[] addKey = Utils.StringToByteArray("add");

            // make sure key does not exist
            table.Delete(addKey);
            client.Submit();

            using (client.Transaction(quorum, majorKey))
            {
                table.Set("x", "x");
                table.Add(addKey, 1);
            }
        }

        static int testLockTimeoutCounter;
        [TestMethod]
        public void TestLockTimeout()
        {
            var client = Utils.GetClient();
            var quorum = client.GetQuorum(quorumName);
            var database = client.GetDatabase(databaseName);
            var table = database.GetTable(tableName);
            var counter = Interlocked.Increment(ref testLockTimeoutCounter);
            byte[] majorKey = Utils.StringToByteArray("TestLockTimeout" + counter);

            table.Delete("x");
            client.Submit();

            try
            {
                client.StartTransaction(quorum, majorKey);
                Thread.Sleep(lockTimeout + 2000);
                table.Set("x", "x");
                client.CommitTransaction();
            }
            catch (SDBPException e)
            {
                return;
            }

            Assert.Fail("Missed lock expire exception");
        }

        static int testLockFailureCounter;
        [TestMethod]
        public void TestLockFailure()
        {
            var client1 = Utils.GetClient();
            var quorum1 = client1.GetQuorum(quorumName);
            var database1 = client1.GetDatabase(databaseName);
            var table1 = database1.GetTable(tableName);

            var client2 = Utils.GetClient();
            var quorum2 = client2.GetQuorum(quorumName);
            var database2 = client2.GetDatabase(databaseName);
            var table2 = database2.GetTable(tableName);

            var counter = Interlocked.Increment(ref testLockFailureCounter);
            byte[] majorKey = Utils.StringToByteArray("TestLockFailure" + counter);

            client1.StartTransaction(quorum1, majorKey);
            try
            {
                client2.StartTransaction(quorum2, majorKey);
            }
            catch (LockTimeoutException e)
            {
                Assert.Fail("");
            }

        }

        static int testLockFailureExceptionCounter;
        [TestMethod]
        public void TestLockFailureException()
        {
            var client1 = Utils.GetClient();
            var quorum1 = client1.GetQuorum(quorumName);
            var database1 = client1.GetDatabase(databaseName);
            var table1 = database1.GetTable(tableName);

            var client2 = Utils.GetClient();
            var quorum2 = client2.GetQuorum(quorumName);
            var database2 = client2.GetDatabase(databaseName);
            var table2 = database2.GetTable(tableName);

            var client3 = Utils.GetClient();
            var quorum3 = client3.GetQuorum(quorumName);
            var database3 = client3.GetDatabase(databaseName);
            var table3 = database3.GetTable(tableName);

            var counter = Interlocked.Increment(ref testLockFailureExceptionCounter);
            byte[] majorKey = Utils.StringToByteArray("TestLockFailureException" + counter);

            client1.StartTransaction(quorum1, majorKey);

            Client[] clients = {client2, client3};
            Quorum[] quorums = {quorum2, quorum3};

            int numExceptions = 0;
            Parallel.For(0, 2, i => {
                try
                {
                    clients[i].StartTransaction(quorums[i], majorKey);
                }
                catch (LockTimeoutException e)
                {
                    Interlocked.Increment(ref numExceptions);
                }
            });
            
            Assert.IsTrue(numExceptions == 1);
        }

        static int testPrimaryFailoverWhileLockHeldCounter;
        [TestMethod]
        public void TestPrimaryFailoverWhileLockHeld()
        {
            var client = Utils.GetClient();
            var quorum = client.GetQuorum(quorumName);
            var database = client.GetDatabase(databaseName);
            var table = database.GetTable(tableName);

            var counter = Interlocked.Increment(ref testLockFailureExceptionCounter);
            byte[] majorKey = Utils.StringToByteArray("TestPrimaryFailoverWhileLockHeld" + counter);

            client.StartTransaction(quorum, majorKey);

            // make primary sleep and lose lease
            var query = ClusterHelpers.SleepAction(debugKey, sleepInterval);
            ClusterHelpers.PrimaryShardServerHTTPAction(client, quorum, query);

            table.Set("x", "x");

            try
            {
                client.CommitTransaction();
            }
            catch (TransactionException e)
            {
                return;
            }

            Assert.Fail("Missed exception due to primary failover");
        }
    }
}
