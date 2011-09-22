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
    }

    public class TestUser : IDisposable
    {
        public TestUserInfo info;

        public TestUser() { }

        public TestUser(long newid)
        {
            info = new TestUserInfo();
            info.id = Users.Id(newid);
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
        private string[] controllers;
        // "192.168.137.50:7080"
        // "192.168.137.50:37080"
        private string dbname = "zszabo_test";
        private string tablename = "User";

        private int client_count;
        private int client_index;
        public Client[] clients;
        private Database db;
        private Table indices;
        private Table table;
        private Table tableByNick;
        private Table tableByBirth;
        private Table tableByLastLogin;
        private Sequence userIDs;

        private MemoryStream _stream;
        private System.Random RandomNumber;

        public Users(string[] c)
        {
            controllers = c;
        }

        private byte[] JsonSerialize(object obj)
        {
            DataContractJsonSerializer jsonSerializer = new DataContractJsonSerializer(obj.GetType());
            _stream.SetLength(0);
            jsonSerializer.WriteObject(_stream, obj);
            return _stream.ToArray();
        }

        private T JsonDeserialize<T>(byte[] data)
        {
            DataContractJsonSerializer jsonSerializer = new DataContractJsonSerializer(typeof(T));
            var stream = new MemoryStream(data);
            T t = (T)jsonSerializer.ReadObject(stream);
            return t;
        }

        public static string Id(Int64 num)
        {
            return String.Format("{0:0000000000000}", num);
        }

        private string RandomString(int size)
        {
            string res = "";
            char ch;

            for (int i = 0; i < size; i++)
            {
                ch = Convert.ToChar(RandomNumber.Next(64, 125));
                res = res + ch;
            }
            return res;
        }

        public void addClient()
        {
            // add, open, client
            // delete dbs on client
        }

        public void get_dbs()
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

        public void init()
        {
            // Client.SetTrace(true);
            clients = new Client[1];

            client_count = 1;
            client_index = 0;

            clients[client_index] = new Client(controllers);

            Utils.deleteDBs(clients[client_index]);

            get_dbs();

            _stream = new MemoryStream();

            RandomNumber = new System.Random();
        }

        public void resetTables()
        {
            indices.TruncateTable();
            // userIDs.Reset();
            table.TruncateTable();
            tableByNick.TruncateTable();
            tableByBirth.TruncateTable();
            tableByLastLogin.TruncateTable();

            // TODO generate rows
        }

        public void addUser()
        {
            // getID from Sequence
            using (TestUser user = new TestUser(userIDs.GetNext))
            {
                user.info.Nick = RandomString(12);
                setUser(user);
            }
        }

        public TestUser getUser(string userID)
        {
            TestUser user = new TestUser();

            byte[] row = table.Get(System.Text.Encoding.UTF8.GetBytes(userID));
            if (row == null) return null;
            user.info = JsonDeserialize<TestUserInfo>(row);

            return user;
        }

        public void setUser(TestUser user)
        {
            bool update = false;
            // read old data for indexes
            TestUser oldVersion = getUser(user.info.id);
            if (oldVersion != null) update = true;

            // last login will be last set :)
            user.info.LastLogin = System.DateTime.Now.ToShortDateString();

            // set user row
            table.Set(System.Text.Encoding.UTF8.GetBytes(user.info.id), JsonSerialize(user.info));

            if (update)
            {
                // set nick index
                if (oldVersion.info.Nick == user.info.Nick)
                {
                    // just set
                    tableByNick.Set(System.Text.Encoding.UTF8.GetBytes(user.info.Nick + "|" + user.info.id.ToString()), JsonSerialize(user.info));
                }
                else
                {
                    // delete old
                    tableByNick.Delete(oldVersion.info.Nick + "|" + user.info.id.ToString());
                    // set new
                    tableByNick.Set(System.Text.Encoding.UTF8.GetBytes(user.info.Nick + "|" + user.info.id.ToString()), JsonSerialize(user.info));
                }

                // set dateofbirth index
                if (oldVersion.info.DateOfBirth == user.info.DateOfBirth)
                {
                    // just set
                    tableByBirth.Set(System.Text.Encoding.UTF8.GetBytes(user.info.DateOfBirth + "|" + user.info.id.ToString()), JsonSerialize(user.info));
                }
                else
                {
                    // delete old
                    tableByBirth.Delete(oldVersion.info.DateOfBirth + "|" + user.info.id.ToString());
                    // set new
                    tableByBirth.Set(System.Text.Encoding.UTF8.GetBytes(user.info.DateOfBirth + "|" + user.info.id.ToString()), JsonSerialize(user.info));
                }

                // set lastlogin index
                if (oldVersion.info.LastLogin == user.info.LastLogin)
                {
                    // just set
                    tableByLastLogin.Set(System.Text.Encoding.UTF8.GetBytes(user.info.LastLogin + "|" + user.info.id.ToString()), JsonSerialize(user.info));
                }
                else
                {
                    // delete old
                    tableByLastLogin.Delete(oldVersion.info.LastLogin + "|" + user.info.id.ToString());
                    // set new
                    tableByLastLogin.Set(System.Text.Encoding.UTF8.GetBytes(user.info.LastLogin + "|" + user.info.id.ToString()), JsonSerialize(user.info));
                }
            }
            else
            {
                tableByNick.Set(System.Text.Encoding.UTF8.GetBytes(user.info.Nick + "|" + user.info.id.ToString()), JsonSerialize(user.info));
                tableByBirth.Set(System.Text.Encoding.UTF8.GetBytes(user.info.DateOfBirth + "|" + user.info.id.ToString()), JsonSerialize(user.info));
                tableByLastLogin.Set(System.Text.Encoding.UTF8.GetBytes(user.info.LastLogin + "|" + user.info.id.ToString()), JsonSerialize(user.info));
            }

        }

        public void deleteUser(TestUser user)
        {
            table.Delete(user.info.id);
            tableByNick.Delete(user.info.Nick + "|" + user.info.id.ToString());
            tableByBirth.Delete(user.info.DateOfBirth + "|" + user.info.id.ToString());
            tableByLastLogin.Delete(user.info.LastLogin + "|" + user.info.id.ToString());
        }

        public long countUsers()
        {
            return (long)table.Count(new StringRangeParams());
        }

        public void printbyNick(string prefix)
        {
            foreach (KeyValuePair<string, string> kv in tableByNick.GetKeyValueIterator(new StringRangeParams().Prefix(prefix)))
            {
                System.Console.WriteLine(kv.Key + " => " + kv.Value);
            }
        }

        public void insertUsers(int cnt)
        {
            while (cnt-- > 0) addUser();
            clients[client_index].Submit();
        }

        public void testCycle(int userNum)
        {
            // uses count for ids, call reset_tables before using this
            long count = countUsers();
            long from = RandomNumber.Next((int)count - userNum);
            TestUser user;

            foreach (string key in table.GetKeyIterator(new StringRangeParams().StartKey(Id(from)).EndKey(Id(from + userNum))))
            {
                System.Console.WriteLine("===========================================================");
                System.Console.WriteLine("Key for test:");
                System.Console.WriteLine(key);

                switch (RandomNumber.Next(5))
                {
                    case 1:
                        // Get user
                        user = getUser(key);
                        System.Console.WriteLine("GetAction:");
                        user.Print();
                        break;
                    case 2:
                        // set user
                        user = getUser(key);
                        System.Console.WriteLine("SetAction:");
                        user.info.Nick = user.info.Nick + "-SetAt" + System.DateTime.Now.ToString("s");
                        setUser(user);
                        user.Print();
                        break;
                    case 3:
                        user = getUser(key);
                        System.Console.WriteLine("DeleteAction:");
                        user.Print();
                        deleteUser(user);
                        // 
                        break;
                    case 4:
                        user = getUser(key);
                        string prefix = user.info.Nick.Substring(0, 3);
                        System.Console.WriteLine("ListSimilar to Nick: " + user.info.Nick + " using prefix: " + prefix);
                        printbyNick(prefix);
                        break;
                    default:
                        //add user
                        System.Console.WriteLine("Add new");
                        addUser();
                        break;
                }
            }
            // random operation on list
        }

        public bool testGetSetSubmit()
        {
            // this reset tables causes a test fail
            resetTables();

            table.Get("0000000000001");
            table.Set("0000000000001", "test");
            table.Get("0000000000002");
            table.Set("0000000000002", "test");

            clients[client_index].Submit();

            var i = table.Count(new ByteRangeParams());
            return i == 2;
        }

        // test entry point
        /*public static void Main(string[] args)
        {
            string[] controllers_conf = { "192.168.137.103:37080", "192.168.137.51:37080", "192.168.137.52:37080" };
            Users uTest = new Users(controllers_conf);

            uTest.init();

            uTest.resetTables();

            uTest.insertUsers(1000);

            uTest.testCycle(10);

            System.Console.ReadLine();
        }*/
    }
}
