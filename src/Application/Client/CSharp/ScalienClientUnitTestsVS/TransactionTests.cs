using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;

#if !SCALIEN_UNIT_TEST_FRAMEWORK
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

using Scalien;
using System.Threading;

namespace ScalienClientUnitTesting
{
    /// <summary>
    /// Summary description for TransactionTests
    /// </summary>
    [TestClass]
    public class TransactionTests
    {
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
            var quorum = client.GetQuorum("Storage");
            var database = client.GetDatabase("test_db");
            var table = database.GetTable("test_table");
            byte[] majorKey = Utils.StringToByteArray("tx");
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
    }
}
