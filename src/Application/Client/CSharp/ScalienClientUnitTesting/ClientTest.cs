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
    public class ClientTest
    {
        [TestMethod]
        public void CreateClientPerRequest()
        {
            var dbName = "test";
            var tableName = "CreateClientPerRequest";

            // make sure the db and table exists
            Assert.IsNotNull(Utils.GetOrCreateTableAndDatabase(new Client(Config.GetNodes()), dbName, tableName));

            // without connection pooling
            Client.SetConnectionPoolSize(0);
            for (var i = 0; i < 100 * 1000; i++)
            {
                Client client = new Client(Config.GetNodes());
                Database db = client.GetDatabase(dbName);
                Table table = db.GetTable(tableName);
                table.Set("" + i, "" + i);
            }

            // with connection pooling
            Client.SetConnectionPoolSize(100);
            for (var i = 0; i < 100 * 1000; i++)
            {
                Client client = new Client(Config.GetNodes());
                Database db = client.GetDatabase(dbName);
                Table table = db.GetTable(tableName);
                table.Set("" + i, "" + i);
            }
        }
    }
}
