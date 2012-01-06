using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Net;
using System.Runtime.Serialization.Json;

namespace Scalien
{
    public class Utils
    {
        public class TestThreadConf
        {
            public List<Exception> exceptionsCatched;
            public object param;

            public TestThreadConf()
            {
                exceptionsCatched = new List<Exception>();
            }
        }

        public static System.Random RandomNumber = new System.Random();

        public class HTTP
        {
            public static string BuildUri(params object[] args)
            {
                string uri = "";

                foreach (var arg in args)
                {
                    if (arg is string)
                        uri += RequestUriString(StringToByteArray((string)arg));
                    else
                        uri += RequestUriString((byte[])arg);
                }

                return uri;
            }

            public static string RequestUriString(byte[] uri)
            {
                string ret = "";

                foreach (var b in uri)
                {
                    if (b < 32 || b >= 128)
                    {
                        ret += String.Format("%{0:00G}", b);
                    }
                    else
                        ret += (char)b;
                }

                return ret;
            }

            public static string GET(string url, int Timeout = 30000)
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
                request.UserAgent = "ScalienClient CSharp";
                request.KeepAlive = false;
                request.Method = "GET";
                request.Timeout = Timeout;
                request.Proxy = null;
                try
                {
                    HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                    using (BufferedStream buffer = new BufferedStream(response.GetResponseStream()))
                    {
                        using (StreamReader reader = new StreamReader(buffer))
                        {
                            return reader.ReadToEnd();
                        }
                    }
                }
                catch (Exception e)
                {
                    return e.Message;
                }
            }

            public static byte[] BinaryGET(string url, int Timeout = 30000)
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
                request.UserAgent = "ScalienClient CSharp";
                request.KeepAlive = false;
                request.Method = "GET";
                request.Timeout = Timeout;
                request.Proxy = null;
                try
                {
                    HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                    using (BufferedStream buffer = new BufferedStream(response.GetResponseStream()))
                    {
                        MemoryStream stream = new MemoryStream();
                        buffer.CopyTo(stream);
                        return stream.ToArray();
                    }
                }
                catch (Exception e)
                {
                    return null;
                }
            }
        }

        public static bool DBCompare(Database DB1, Database DB2)
        {
            bool res = true;
            Table cmp_tbl;
            byte[] cmp_val;

            // DB1 -> DB2
            // call the function vice versa too, to be sure in result
            List<Table> tables = DB1.GetTables();
            foreach (Table tbl in tables)
            {
                if (null == (cmp_tbl = DB2.GetTable(tbl.Name)))
                {
                    Console.WriteLine(tbl.Name + " table is not in database " + DB2.Name);
                    res = false;
                }
                else
                {
                    Console.WriteLine("Comparing table " + tbl.Name);

                    // check for table data
                    foreach (KeyValuePair<byte[], byte[]> kv in tbl.GetKeyValueIterator(new ByteRangeParams()))
                    {
                        cmp_val = cmp_tbl.Get(kv.Key);

                        if (null == cmp_val)
                        {
                            Console.WriteLine("Key \"" + kv.Key.ToString() + "\" is not in table " + tbl.Name);
                            res = false;
                            continue;
                        }

                        if (!Utils.ByteArraysEqual(kv.Value, cmp_val))
                        {
                            Console.WriteLine("Values for key \"" + kv.Key.ToString() + "\" doesn't match:");
                            Console.WriteLine(kv.Value.ToString());
                            Console.WriteLine(cmp_val.ToString());
                            res = false;
                            continue;
                        }
                    }
                }
            }

            return res;
        }

        public static byte[] JsonSerialize(object obj)
        {
            MemoryStream _stream = new MemoryStream();
            DataContractJsonSerializer jsonSerializer = new DataContractJsonSerializer(obj.GetType());
            _stream.SetLength(0);
            jsonSerializer.WriteObject(_stream, obj);
            return _stream.ToArray();
        }

        public static T JsonDeserialize<T>(byte[] data)
        {
            DataContractJsonSerializer jsonSerializer = new DataContractJsonSerializer(typeof(T));
            var stream = new MemoryStream(data);
            T t = (T)jsonSerializer.ReadObject(stream);
            return t;
        }

        public static string Id(UInt64 num)
        {
            return String.Format("{0:0000000000000}", num);
        }

        public static void DeleteAllDatabases(Client c)
        {
            foreach (Database db in c.GetDatabases())
                db.DeleteDatabase();
        }

        public static Table GetOrCreateTableAndDatabase(Client client, string dbName, string tableName)
        {
            Database db = null;
            try
            {
                db = client.GetDatabase(dbName);
            }
            catch (SDBPException e)
            {
                if (e.Status == Status.SDBP_BADSCHEMA)
                    db = client.CreateDatabase(dbName);
            }
            if (db == null)
                return null;

            Table table = null;
            try
            {
                table = db.GetTable(tableName);
            }
            catch (SDBPException e)
            {
                if (e.Status == Status.SDBP_BADSCHEMA)
                    table = db.CreateTable(tableName);
            }

            if (table == null)
                table = db.CreateTable(tableName);

            return table;
        }

        public static Table GetOrCreateEmptyTableAndDatabase(Client client, string dbName, string tableName)
        {
            Table table = GetOrCreateTableAndDatabase(client, dbName, tableName);
            if (table == null)
                return null;

            table.TruncateTable();

            return table;
        }

        public static Database GetOrCreateEmptyDatabase(Client client, string dbName)
        {
            Database db = null;
            try
            {
                db = client.GetDatabase(dbName);
            }
            catch (SDBPException e)
            {
                if (e.Status == Status.SDBP_BADSCHEMA)
                {
                    db = client.CreateDatabase(dbName);
                    return db;
                }

            }
            if (db == null)
                return null;

            foreach (Table table in db.GetTables())
            {
                table.DeleteTable();
            }

            return db;
        }


        public static string RandomString(int size = 0)
        {
            string res = "";
            char ch;

            if (size == 0) size = RandomNumber.Next(5, 50);

            for (int i = 0; i < size; i++)
            {
                ch = Convert.ToChar(RandomNumber.Next(33, 126));
                res = res + ch;
            }
            return res;
        }

        // unique guarantees that ascii is unique in past ~35 minutes
        public static byte[] RandomASCII(int size = 0, bool unique = false)
        {
            byte[] res;
            Int64 mil;

            if (size == 0) size = RandomNumber.Next(5, 50);

            res = new byte[size];

            if (unique)
            {
                for (int i = 0; i < size; i++)
                    res[i] = (byte)RandomNumber.Next(0, 127);
            }
            else
            {
                for (int i = 0; i < size - 3; i++)
                    res[i] = (byte)RandomNumber.Next(0, 127);

                mil = System.DateTime.Now.Millisecond;
                res[size - 3] = (byte)System.Convert.ToSByte((mil / 16384) % 128);
                res[size - 2] = (byte)System.Convert.ToSByte((mil / 128) % 128);
                res[size - 1] = (byte)System.Convert.ToSByte(mil % 128);
            }

            return res;
        }

        public static byte[] ReadFile(string filePath)
        {
            byte[] buffer = null;
            FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            try
            {
                int length = (int)fileStream.Length;  // get file length
                buffer = new byte[length];            // create buffer
                int count;                            // actual number of bytes read
                int sum = 0;                          // total number of bytes read

                // read until Read method returns 0 (end of the stream has been reached)
                while ((count = fileStream.Read(buffer, sum, length - sum)) > 0)
                    sum += count;  // sum is a buffer offset for next reading
            }
            finally
            {
                fileStream.Close();
            }
            return buffer;
        }

        public static bool WriteFile(string filePath, byte[] data)
        {
            bool res = true;
            
            FileStream fileStream = new FileStream(filePath, FileMode.CreateNew, FileAccess.Write);
            try
            {
                // TODO block
                fileStream.Write(data, 0, data.Length);
            }
            catch (IOException exp)
            {
                res = false;
                throw exp;
            }
            finally
            {
                fileStream.Close();
            }

            return res;
        }

        public static bool ByteArraysEqual(byte[] a, byte[] b)
        {
            if (a.GetLength(0) != b.GetLength(0)) return false;

            for (int i = 0; i < a.GetLength(0); i++)
                if (a[i] != b[i]) return false;

            return true;
        }

        public static bool NotLargerThan(byte[] s, byte[] l)
        {
            for (int i = 0; i < s.Length && i < l.Length; i++)
            {
                if (s[i] == l[i]) continue;

                if (s[i] < l[i]) return true;
                else return false;
            }

            if (s.Length > l.Length) return false;

            return true;
        }

        public static void SortKeyValueArrays(ref byte[][] keys, ref byte[][] values, int len)
        {
            int minid;
            byte[] tmp;

            for (int i = 0; i < len; i++)
            {
                minid = i;

                for (int j = i; j < len; j++)
                {
                    if (NotLargerThan(keys[j], keys[minid])) minid = j;
                }

                tmp = keys[minid];
                keys[minid] = keys[i];
                keys[i] = tmp;

                tmp = values[minid];
                values[minid] = values[i];
                values[i] = tmp;
            }
        }

        /// <summary>
        /// Helper function for converting string type to byte[]
        /// </summary>
        public static byte[] StringToByteArray(string str)
        {
            return System.Text.Encoding.UTF8.GetBytes(str);
        }


        public static string[] GetConfigNodes()
        {
            return new ConfigFile().GetStringArrayValue("controllers");
        }
    }
}