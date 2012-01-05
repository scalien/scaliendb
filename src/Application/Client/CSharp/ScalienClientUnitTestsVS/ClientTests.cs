using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;

#if !SCALIEN_UNIT_TEST_FRAMEWORK
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

using Scalien;

namespace ScalienClientUnitTesting
{
    /// <summary>
    /// Summary description for ClientTest
    /// </summary>
    [TestClass]
    public class ClientTests
    {
        [TestMethod]
        public void CreateClientPerRequest()
        {
            var dbName = "test";
            var tableName = "CreateClientPerRequest";

            // make sure the db and table exists
            Assert.IsNotNull(Utils.GetOrCreateTableAndDatabase(new Client(Utils.GetConfigNodes()), dbName, tableName));

            // without connection pooling
            Client.SetConnectionPoolSize(0);
            for (var i = 0; i < 100 * 1000; i++)
            {
                Client client = new Client(Utils.GetConfigNodes());
                Database db = client.GetDatabase(dbName);
                Table table = db.GetTable(tableName);
                table.Set("" + i, "" + i);
            }

            // with connection pooling
            Client.SetConnectionPoolSize(100);
            for (var i = 0; i < 100 * 1000; i++)
            {
                Client client = new Client(Utils.GetConfigNodes());
                Database db = client.GetDatabase(dbName);
                Table table = db.GetTable(tableName);
                table.Set("" + i, "" + i);
            }
        }
        
        [TestMethod]
        public void TestMaxConnections()
        {
            var dbName = "test";
            var tableName = "CreateClientPerRequest";

            // make sure the db and table exists
            Assert.IsNotNull(Utils.GetOrCreateTableAndDatabase(new Client(Utils.GetConfigNodes()), dbName, tableName));

            Client.SetMaxConnections(10);

            for (var i = 0; i < 11; i++)
            {
                Client client = new Client(Utils.GetConfigNodes());
                client.SetGlobalTimeout(15000);
                client.SetMasterTimeout(10000);
                Database db = client.GetDatabase(dbName);
                Table table = db.GetTable(tableName);
                try
                {
                    table.Set("" + i, "" + i);
                    client.Submit();
                }
                catch (SDBPException e)
                {
                    Assert.IsTrue(i == 10 && e.Status == Status.SDBP_FAILURE);
                }
            }

        }
    }
}
