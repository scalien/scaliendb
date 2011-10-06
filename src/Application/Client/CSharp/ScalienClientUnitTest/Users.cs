using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Json;

namespace Scalien
{
    public class TestUserInfo
    {
        public string id;
        public string Nick;
        public string DateOfBirth;
        public string LastLogin;

        public static bool operator ==(TestUserInfo ui1, TestUserInfo ui2)
        {
            return ((ui1.id.Equals(ui2.id)) && (ui1.Nick.Equals(ui2.Nick)) && (ui1.DateOfBirth.Equals(ui2.DateOfBirth)) && (ui1.LastLogin.Equals(ui2.LastLogin))) ;
        }

        public static bool operator !=(TestUserInfo ui1, TestUserInfo ui2)
        {
            return !((ui1.id.Equals(ui2.id)) && (ui1.Nick.Equals(ui2.Nick)) && (ui1.DateOfBirth.Equals(ui2.DateOfBirth)) && (ui1.LastLogin.Equals(ui2.LastLogin)));
        }
    }

    public class TestUser : IDisposable
    {
        public TestUserInfo info;

        public TestUser() { }

        public TestUser(long newid)
        {
            info = new TestUserInfo();
            info.id = Utils.Id(newid);
            info.DateOfBirth = System.DateTime.Now.ToShortDateString();
            info.LastLogin = System.DateTime.Now.ToShortDateString();
        }

        public void Print()
        {
            System.Console.WriteLine("ID: " + info.id + " Nick: " + info.Nick + " DateOfBirth: " + info.DateOfBirth + " LastLogin: " + info.DateOfBirth);
        }

        public void Dispose()
        {
        }
    }

    public class Users
    {
        private string dbname = "users_test";
        private string tablename = "User";

        private int ciinum;
        private int client_index;
        private List<Client> clients = new List<Client>();
        private Database db;
        private Table indices;
        private Table table;
        private Table tableByNick;
        private Table tableByBirth;
        private Table tableByLastLogin;
        private Sequence userIDs;

        public Users(string[] nodes = null)
        {
            client_index = ciinum = 0;
            if (nodes != null)
            {
                AddClient(nodes);
                OpenDB();
            }
        }

        public void AddClient(string[] nodes)
        {
            Client newclient = new Client(nodes);

            clients.Add(newclient);
        }

        public Client GetClient()
        {
            return clients[client_index];
        }

        public void EmptyAll()
        {
            while(IterateClients())
                Utils.DeleteDBs(clients[client_index]);
            OpenDB();
        }

        public void SubmitAll()
        {
            while (IterateClients())
                clients[client_index].Submit();
        }

        private void OpenDB()
        {
            db = clients[client_index].GetDatabase(dbname);
            if (db == null)
                db = clients[client_index].CreateDatabase(dbname);

            indices = db.GetTable("indices");
            if (indices == null)
                indices = db.CreateTable("indices");

            userIDs = indices.GetSequence("userIDs");

            table = db.GetTable(tablename);
            if (table == null)
                table = db.CreateTable(tablename);

            tableByNick = db.GetTable(tablename + "ByNick");
            if (tableByNick == null)
                tableByNick = db.CreateTable(tablename + "ByNick");

            tableByBirth = db.GetTable(tablename + "ByBirth");
            if (tableByBirth == null)
                tableByBirth = db.CreateTable(tablename + "ByBirth");

            tableByLastLogin = db.GetTable(tablename + "ByLastLogin");
            if (tableByLastLogin == null)
                tableByLastLogin = db.CreateTable(tablename + "ByLastLogin");
        }

        public bool IterateClients()
        {
            if (clients.Count < ciinum + 1)
            {
                // TODO: force IterateClients usage
                ciinum = 0;
                return false;
            }

            client_index = ciinum;

            OpenDB();

            ciinum++;

            return true;
        }

        public void ResetTables()
        {
            indices.TruncateTable();
            table.TruncateTable();
            tableByNick.TruncateTable();
            tableByBirth.TruncateTable();
            tableByLastLogin.TruncateTable();

            userIDs.Reset();
        }

        public void AddUser()
        {
            // getID from Sequence
            using (TestUser user = new TestUser(userIDs.GetNext))
            {
                user.info.Nick = Utils.RandomString(12);
                SetUser(user);
            }
        }

        public TestUser GetUser(string userID)
        {
            TestUser user = new TestUser();

            byte[] row = table.Get(System.Text.Encoding.UTF8.GetBytes(userID));
            if (row == null) return null;
            user.info = Utils.JsonDeserialize<TestUserInfo>(row);

            return user;
        }

        public void SetUser(TestUser user)
        {
            bool update = false;
            // read old data for indexes
            TestUser oldVersion = GetUser(user.info.id);
            if (oldVersion != null) update = true;

            // last login will be last set :)
            user.info.LastLogin = System.DateTime.Now.ToShortDateString();

            // set user row
            table.Set(System.Text.Encoding.UTF8.GetBytes(user.info.id), Utils.JsonSerialize(user.info));

            if (update)
            {
                // set nick index
                if (oldVersion.info.Nick == user.info.Nick)
                {
                    // just set
                    tableByNick.Set(System.Text.Encoding.UTF8.GetBytes(user.info.Nick + "|" + user.info.id.ToString()), Utils.JsonSerialize(user.info));
                }
                else
                {
                    // delete old
                    tableByNick.Delete(oldVersion.info.Nick + "|" + user.info.id.ToString());
                    // set new
                    tableByNick.Set(System.Text.Encoding.UTF8.GetBytes(user.info.Nick + "|" + user.info.id.ToString()), Utils.JsonSerialize(user.info));
                }

                // set dateofbirth index
                if (oldVersion.info.DateOfBirth == user.info.DateOfBirth)
                {
                    // just set
                    tableByBirth.Set(System.Text.Encoding.UTF8.GetBytes(user.info.DateOfBirth + "|" + user.info.id.ToString()), Utils.JsonSerialize(user.info));
                }
                else
                {
                    // delete old
                    tableByBirth.Delete(oldVersion.info.DateOfBirth + "|" + user.info.id.ToString());
                    // set new
                    tableByBirth.Set(System.Text.Encoding.UTF8.GetBytes(user.info.DateOfBirth + "|" + user.info.id.ToString()), Utils.JsonSerialize(user.info));
                }

                // set lastlogin index
                if (oldVersion.info.LastLogin == user.info.LastLogin)
                {
                    // just set
                    tableByLastLogin.Set(System.Text.Encoding.UTF8.GetBytes(user.info.LastLogin + "|" + user.info.id.ToString()), Utils.JsonSerialize(user.info));
                }
                else
                {
                    // delete old
                    tableByLastLogin.Delete(oldVersion.info.LastLogin + "|" + user.info.id.ToString());
                    // set new
                    tableByLastLogin.Set(System.Text.Encoding.UTF8.GetBytes(user.info.LastLogin + "|" + user.info.id.ToString()), Utils.JsonSerialize(user.info));
                }
            }
            else
            {
                tableByNick.Set(System.Text.Encoding.UTF8.GetBytes(user.info.Nick + "|" + user.info.id.ToString()), Utils.JsonSerialize(user.info));
                tableByBirth.Set(System.Text.Encoding.UTF8.GetBytes(user.info.DateOfBirth + "|" + user.info.id.ToString()), Utils.JsonSerialize(user.info));
                tableByLastLogin.Set(System.Text.Encoding.UTF8.GetBytes(user.info.LastLogin + "|" + user.info.id.ToString()), Utils.JsonSerialize(user.info));
            }
        }

        public void DeleteUser(TestUser user)
        {
            table.Delete(user.info.id);
            tableByNick.Delete(user.info.Nick + "|" + user.info.id.ToString());
            tableByBirth.Delete(user.info.DateOfBirth + "|" + user.info.id.ToString());
            tableByLastLogin.Delete(user.info.LastLogin + "|" + user.info.id.ToString());
        }

        public long CountUsers()
        {
            return (long)table.Count(new ByteRangeParams());
        }

        public void PrintByNick(string prefix)
        {
            foreach (KeyValuePair<string, string> kv in tableByNick.GetKeyValueIterator(new StringRangeParams().Prefix(prefix)))
            {
                System.Console.WriteLine(kv.Key + " => " + kv.Value);
            }
        }

        public void InsertUsers(int cnt)
        {
            while (cnt-- > 0) AddUser();
            clients[client_index].Submit();
        }

        public bool IsConsistent()
        {
            TestUser user;
            TestUserInfo cmpinfo;
            byte[] row;

            // byte array?
            foreach (string key in table.GetKeyIterator(new StringRangeParams()))
            {
                user = GetUser(key);

                row = tableByNick.Get(System.Text.Encoding.UTF8.GetBytes(user.info.Nick + "|" + user.info.id.ToString()));
                if (row == null) return false;
                cmpinfo = Utils.JsonDeserialize<TestUserInfo>(row);
                if (user.info != cmpinfo) 
                    return false;

                row = tableByBirth.Get(System.Text.Encoding.UTF8.GetBytes(user.info.DateOfBirth + "|" + user.info.id.ToString()));
                if (row == null) return false;
                cmpinfo = Utils.JsonDeserialize<TestUserInfo>(row);
                if (user.info != cmpinfo) 
                    return false;

                row = tableByLastLogin.Get(System.Text.Encoding.UTF8.GetBytes(user.info.LastLogin + "|" + user.info.id.ToString()));
                if (row == null) return false;
                cmpinfo = Utils.JsonDeserialize<TestUserInfo>(row);
                if (user.info != cmpinfo) 
                    return false;
            }

            return true;
        }

        public void TestCycle(int userNum)
        {
            // uses count for ids, call reset_tables before using this
            long count = CountUsers();
            long from = Utils.RandomNumber.Next((int)count - userNum);
            TestUser user;

            foreach (string key in table.GetKeyIterator(new StringRangeParams().StartKey(Utils.Id(from)).Count((uint)userNum)))
            {
                //System.Console.WriteLine("===========================================================");
                //System.Console.WriteLine("Key for test:");
                //System.Console.WriteLine(key);
                System.Console.Write(".");

                switch (Utils.RandomNumber.Next(6))
                {
                    case 1:
                        // Get user
                        user = GetUser(key);
                        //System.Console.WriteLine("GetAction:");
                        //user.Print();
                        break;
                    case 2:
                        // set user
                        user = GetUser(key);
                        //System.Console.WriteLine("SetAction:");
                        user.info.Nick = user.info.Nick + "-SetAt" + System.DateTime.Now.ToString("s");
                        SetUser(user);
                        //user.Print();
                        break;
                    /*case 3:
                        user = GetUser(key);
                        System.Console.WriteLine("DeleteAction:");
                        user.Print();
                        DeleteUser(user);
                        // 
                        break;*/
                    case 4:
                        user = GetUser(key);
                        string prefix = user.info.Nick.Substring(0, 3);
                        //System.Console.WriteLine("ListSimilar to Nick: " + user.info.Nick + " using prefix: " + prefix);
                        //PrintByNick(prefix);
                        break;
                    default:
                        //add user
                        //System.Console.WriteLine("Add new");
                        AddUser();
                        break;
                }
                clients[client_index].Submit();
            }
            // TODO more operations
            // random operation on list
        }

        public void TestCycle_MultiUser()
        {
            int operation = Utils.RandomNumber.Next(15);
            long count = CountUsers(); // we hope counts are the same
            long from = Utils.RandomNumber.Next((int)count - 1);
            TestUser user;
            
            while (IterateClients())
            {
                foreach (string key in table.GetKeyIterator(new StringRangeParams().StartKey(Utils.Id(from)).Count(1)))
                {
                    System.Console.WriteLine("===========================================================");
                    System.Console.WriteLine("Key for test:");
                    System.Console.WriteLine(key);

                    switch (operation)
                    {
                        case 1:
                            // Get user
                            user = GetUser(key);
                            System.Console.WriteLine("GetAction:");
                            user.Print();
                            break;
                        case 2:
                            // set user
                            user = GetUser(key);
                            System.Console.WriteLine("SetAction:");
                            user.info.Nick = user.info.Nick + "-SetAt" + System.DateTime.Now.ToString("s");
                            SetUser(user);
                            user.Print();
                            break;
                        case 3:
                            user = GetUser(key);
                            System.Console.WriteLine("DeleteAction:");
                            user.Print();
                            DeleteUser(user);
                            // 
                            break;
                        case 4:
                            user = GetUser(key);
                            string prefix = user.info.Nick.Substring(0, 3);
                            System.Console.WriteLine("ListSimilar to Nick: " + user.info.Nick + " using prefix: " + prefix);
                            PrintByNick(prefix);
                            break;
                        default:
                            //add user
                            System.Console.WriteLine("Add new");
                            AddUser();
                            break;
                    }
                }
            }

            SubmitAll();
        }

        public bool ClientDBsAreSame(int client_index_a, int client_index_b)
        {
            bool res = true;

            if (clients.Count < Math.Max(client_index_a, client_index_b) + 1) return false;

            Database dba = clients[client_index_a].GetDatabase(dbname);
            Database dbb = clients[client_index_b].GetDatabase(dbname);

            if (!Utils.DBCompare(dba, dbb))
            {
                Console.WriteLine("Database on client " + client_index_b + " differs from reference db on " + client_index_a);
                res = false;
            }

            if (!Utils.DBCompare(dbb, dba))
            {
                Console.WriteLine("Database on client " + client_index_a + " differs from reference db on " + client_index_b);
                res = false;
            }

            return res;
        }

        public bool TestGetSetSubmit()
        {
            // this reset tables causes a test fail
            ResetTables();

            table.Get("0000000000001");
            table.Set("0000000000001", "test");
            table.Get("0000000000002");
            table.Set("0000000000002", "test");

            clients[client_index].Submit();

            var i = table.Count(new ByteRangeParams());
            return i == 2;
        }
    }
}
