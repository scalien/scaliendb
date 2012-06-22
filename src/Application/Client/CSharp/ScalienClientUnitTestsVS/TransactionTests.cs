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
        public static int lockTimeout = 3 * 1000;

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

        [TestMethod]
        public void TestAddNotExisting()
        {
            var client = Utils.GetClient();
            var quorum = client.GetQuorum(quorumName);
            var database = client.GetDatabase(databaseName);
            var table = database.GetTable(tableName);
            byte[] majorKey = Utils.StringToByteArray("TestAddNotExisting");
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

        [TestMethod]
        public void TestLockTimeout()
        {
            var client = Utils.GetClient();
            var quorum = client.GetQuorum(quorumName);
            var database = client.GetDatabase(databaseName);
            var table = database.GetTable(tableName);
            byte[] majorKey = Utils.StringToByteArray("TestLockTimeout");

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

            byte[] majorKey = Utils.StringToByteArray("TestLockFailure");

            client1.StartTransaction(quorum1, majorKey);
            try
            {
                client2.StartTransaction(quorum2, majorKey);
            }
            catch (TransactionException e)
            {
                Assert.Fail("");
            }

        }

        static int testLockFailureException_counter;
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

            //Client.SetTrace(true);

            testLockFailureException_counter += 1;
            byte[] majorKey = Utils.StringToByteArray("TestLockFailureException" + testLockFailureException_counter);

            client1.StartTransaction(quorum1, majorKey);

            Client[] clients = {client2, client3};
            Quorum[] quorums = {quorum2, quorum3};

            int numExceptions = 0;
            Parallel.For(0, 2, i => {
                try
                {
                    clients[i].StartTransaction(quorums[i], majorKey);
                }
                catch (TransactionException e)
                {
                    Interlocked.Increment(ref numExceptions);
                }
            });
            
            Assert.IsTrue(numExceptions == 1);
        }

    }
}
