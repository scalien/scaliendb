using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Scalien;

namespace ScalienClientUnitTesting
{
    [TestClass]
    class SchemaTests
    {
        //[TestMethod]
        public void DeleteSomeDBs()
        {
            /*
             * Client scalien = context.getScalien ();
            for (Database database: scalien.getDatabases ())
                if (database.getName ().equals (DATABASE)) {
                    database.deleteDatabase ();
                    break;
                }*/

            string dbName = "manydb_test_";
            Client client = new Client(Config.GetNodes());

            Utils.DeleteDBs(client);

            Database[] db = new Database[500];

            for (int i = 0; i < 500; i++)
                client.CreateDatabase(dbName + i.ToString());

            client.Submit();

            while (client.GetDatabases().Count > 10)
            {
                foreach (Database db2del in client.GetDatabases())
                    if (Utils.RandomNumber.Next(10) < 5)
                    {
                        db2del.DeleteDatabase();
                        break; //?
                    }
            }
        }

        //[TestMethod]
        public void ManyDBs100()
        {
            string dbName = "manydb_test_";
            Client client = new Client(Config.GetNodes());

            Utils.DeleteDBs(client);

            Database[] db = new Database[100];

            for (int i = 0; i < 100; i++)
                client.CreateDatabase(dbName + i.ToString());

            for (int i = 0; i < 100; i++)
            {
                db[i] = client.GetDatabase(dbName + i.ToString());
                Assert.IsNotNull(db[i]);
            }
        }

        //[TestMethod]
        public void ManyDBs1000()
        {
            string dbName = "manydb_test_";

            Client client = new Client(Config.GetNodes());

            Utils.DeleteDBs(client);

            Database[] db = new Database[1000];

            for (int i = 0; i < 1000; i++)
                client.CreateDatabase(dbName + i.ToString());

            for (int i = 0; i < 1000; i++)
            {
                db[i] = client.GetDatabase(dbName + i.ToString());
                Assert.IsNotNull(db[i]);
            }
        }

        //[TestMethod]
        public void ManyTables1000()
        {
            string dbName = "manytables_test";
            string tableName = "manytables_test_";

            Client client = new Client(Config.GetNodes());

            Utils.DeleteDBs(client);

            Database db = client.CreateDatabase(dbName);

            Table[] tbl = new Table[1000];

            for (int i = 0; i < 1000; i++)
                db.CreateTable(tableName + i.ToString());

            for (int i = 0; i < 1000; i++)
            {
                tbl[i] = db.GetTable(tableName + i.ToString());
                Assert.IsNotNull(tbl[i]);
            }
        }

       // [TestMethod]
        public void ManyTables10000()
        {
            string dbName = "manytables_test";
            string tableName = "manytables_test_";

            Client client = new Client(Config.GetNodes());

            Utils.DeleteDBs(client);

            Database db = client.CreateDatabase(dbName);

            Table[] tbl = new Table[10000];

            for (int i = 0; i < 10000; i++)
                db.CreateTable(tableName + i.ToString());

            for (int i = 0; i < 10000; i++)
            {
                tbl[i] = db.GetTable(tableName + i.ToString());
                Assert.IsNotNull(tbl[i]);
            }
        }

        //[TestMethod]
        public void ManyDB_AND_Tables_100x100()
        {
            string dbName = "manydbtables_test_";
            string tableName = "manydbtables_test_";

            Client client = new Client(Config.GetNodes());

            Utils.DeleteDBs(client);

            Database[] db = new Database[100];
            Table[,] tbl = new Table[100, 100];

            for (int i = 0; i < 100; i++)
                client.CreateDatabase(dbName + i.ToString());

            for (int i = 0; i < 100; i++)
            {
                db[i] = client.GetDatabase(dbName + i.ToString());
                Assert.IsNotNull(db[i]);

                // create tables in db
                for (int j = 0; j < 100; j++)
                    db[i].CreateTable(tableName + i.ToString() + "_" + j.ToString());

                for (int j = 0; j < 100; j++)
                {
                    tbl[i, j] = db[i].GetTable(tableName + i.ToString() + "_" + j.ToString());
                    Assert.IsNotNull(tbl[i, j]);
                }
            }
        }

        //[TestMethod]
        public void RandomNamedDBs()
        {
            Client client = new Client(Config.GetNodes());

            Utils.DeleteDBs(client);

            Database[] db = new Database[100];
            string[] dbNames = new string[100];

            for (int i = 0; i < 100; i++)
            {
                dbNames[i] = Utils.RandomString();
                client.CreateDatabase(dbNames[i]);
            }

            for (int i = 0; i < 100; i++)
            {
                db[i] = client.GetDatabase(dbNames[i]);
                Assert.IsNotNull(db[i]);
            }
        }

        //[TestMethod]
        public void RandomNamedDBs_CheckByDBList()
        {
            Client client = new Client(Config.GetNodes());

            Utils.DeleteDBs(client);

            Database[] db = new Database[100];
            List<string> dbNames = new List<string>();

            for (int i = 0; i < 100; i++)
            {
                dbNames.Add(Utils.RandomString());
                client.CreateDatabase(dbNames[i]);
            }

            int j = 0;
            foreach (Database db2chk in client.GetDatabases())
            {
                Assert.IsTrue(db2chk.Name.Equals(dbNames[j++]));
            }
        }

        //[TestMethod]
        public void RandomNamedTables()
        {
            string dbName = "random_named_tables_db";
            string[] tableNames = new string[100];

            Client client = new Client(Config.GetNodes());

            Utils.DeleteDBs(client);

            Database db = client.CreateDatabase(dbName);

            Table[] tbl = new Table[100];

            for (int i = 0; i < 100; i++)
            {
                tableNames[i] = Utils.RandomString();
                db.CreateTable(tableNames[i]);
            }

            for (int i = 0; i < 100; i++)
            {
                tbl[i] = db.GetTable(tableNames[i]);
                Assert.IsNotNull(tbl[i]);
            }
        }

        //[TestMethod]
        public void RandomNamedTables_CheckByTableList()
        {
            string dbName = "random_named_tables_db";
            List<string> tableNames = new List<string>(100);

            Client client = new Client(Config.GetNodes());

            Utils.DeleteDBs(client);

            Database db = client.CreateDatabase(dbName);

            Table[] tbl = new Table[100];

            for (int i = 0; i < 100; i++)
            {
                tableNames.Add(Utils.RandomString());
                db.CreateTable(tableNames[i]);
            }

            int j = 0;
            foreach (Table tbl2chk in db.GetTables())
            {
                Assert.IsTrue(tbl2chk.Name.Equals(tableNames[j++]));
            }
        }

        /*[TestMethod]
        public void RenameDBs()
        {
            string dbName = "rename_db_";
            Client client = new Client(Config.GetNodes());

            Utils.DeleteDBs(client);

            Database[] db = new Database[100];
            string[] dbNames = new string[100];

            for (int i = 0; i < 100; i++)
            {
                dbNames[i] = dbName + i.ToString();
                client.CreateDatabase(dbNames[i]);
            }

            for (int i = 0; i < 100; i++)
            {
                db[i] = client.GetDatabase(dbNames[i]);
                Assert.IsNotNull(db[i]);

                dbNames[i] = dbName + i.ToString() + "_renamed";
                db[i].RenameDatabase(dbNames[i]);
            }

            for (int i = 0; i < 100; i++)
            {
                db[i] = client.GetDatabase(dbNames[i]);
                Assert.IsNotNull(db[i]);
            }
        }

        [TestMethod]
        public void RenameTables()
        {
            string dbName = "rename_tables_db";
            string tblName = "rename_tables_table_";
            string[] tableNames = new string[100];

            Client client = new Client(Config.GetNodes());

            Utils.DeleteDBs(client);

            Database db = client.CreateDatabase(dbName);

            Table[] tbl = new Table[100];

            for (int i = 0; i < 100; i++)
            {
                tableNames[i] = tblName + i.ToString();
                db.CreateTable(tableNames[i]);
            }

            for (int i = 0; i < 100; i++)
            {
                tbl[i] = db.GetTable(tableNames[i]);
                Assert.IsNotNull(tbl[i]);

                tableNames[i] = tblName + i.ToString() + "_renamed";
                tbl[i].RenameTable(tableNames[i]);
            }

            for (int i = 0; i < 100; i++)
            {
                tbl[i] = db.GetTable(tableNames[i]);
                Assert.IsNotNull(tbl[i]);
            }
        }

        [TestMethod]
        public void RenameRandomNamedTables()
        {
            string dbName = "random_named_tables_db";
            string[] tableNames = new string[100];

            Client client = new Client(Config.GetNodes());

            Utils.DeleteDBs(client);

            Database db = client.CreateDatabase(dbName);

            Table[] tbl = new Table[100];

            for (int i = 0; i < 100; i++)
            {
                tableNames[i] = Utils.RandomString();
                db.CreateTable(tableNames[i]);
            }

            for (int i = 0; i < 100; i++)
            {
                tbl[i] = db.GetTable(tableNames[i]);
                Assert.IsNotNull(tbl[i]);

                tableNames[i] = Utils.RandomString();
                tbl[i].RenameTable(tableNames[i]);
            }

            for (int i = 0; i < 100; i++)
            {
                tbl[i] = db.GetTable(tableNames[i]);
                Assert.IsNotNull(tbl[i]);
            }
        }
        /*
        [TestMethod]
        public void TruncateTables()
        {
            // create tables
            // insert data into them
            // truncate them
            // check 
            // TODO
        }*/
    }
}
