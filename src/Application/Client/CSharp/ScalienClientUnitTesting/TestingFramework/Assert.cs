using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ScalienClientUnitTesting
{
    class UnitTestException : Exception
    {
        private string message;

        public UnitTestException()
        {
            message = "";
        }
        public UnitTestException(string msg)
        {
            message = msg;
        }
        public string GetMessage()
        {
            return message;
        }
    }

    class Assert
    {
        public static void Throw(string message)
        {
            throw new UnitTestException(message);
        }
        public static void IsTrue(bool cond)
        {
            if (cond != true) Throw("Assert.IsTrue failed");
        }
        public static void IsTrue(bool cond, string message)
        {
            if (cond != true) Throw(message);
        }
        public static void IsFalse(bool cond)
        {
            if (cond != false) Throw("Assert.IsFalse failed");
        }
        public static void IsFalse(bool cond, string message)
        {
            if (cond != false) Throw(message);
        }
        public static void IsNull(object value)
        {
            if (value != null) Throw("Assert.IsNull failed");
        }
        public static void IsNull(object value, string message)
        {
            if (value != null) Throw(message);
        }
        public static void IsNotNull(object value)
        {
            if (value == null) Throw("Assert.IsNotNull failed");
        }
        public static void IsNotNull(object value, string message)
        {
            if (value == null) Throw(message);
        }
        public static void Fail(string message)
        {
            Throw(message);
        }
        public static void Fail(string message, object obj)
        {
            Throw(message);
        }
        public static void AreEqual<T>(T left, T right, string message)
        {
            if (!left.Equals(right))
                Throw(message);
        }
    }
}
