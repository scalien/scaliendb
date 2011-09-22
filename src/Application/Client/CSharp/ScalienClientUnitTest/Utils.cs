using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Scalien
{
    class Utils
    {
        public static System.Random RandomNumber = new System.Random();

        public static void deleteDBs(Client c)
        {
            foreach (Database db in c.GetDatabases())
                db.DeleteDatabase();
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

        public static byte[] RandomASCII(int size = 0)
        {
            byte[] res;

            if (size == 0) size = RandomNumber.Next(5, 50);

            res = new byte[size];

            for (int i = 0; i < size; i++)
                res[i] = (byte)RandomNumber.Next(0, 127);

            return res;
        }

        public static byte[] ReadFile(string filePath)
        {
            byte[] buffer;
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

        public static bool byteArraysEqual(byte[] a, byte[] b)
        {
            if (a.GetLength(0) != b.GetLength(0)) return false;

            for (int i = 0; i < a.GetLength(0); i++)
                if (a[i] != b[i]) return false;

            return true;
        }
    }
}