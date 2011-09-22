using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;

namespace ScalienClientUnitTesting
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class TestClassAttribute : Attribute
    {
        public TestClassAttribute()
        {
        }
    }

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public class TestMethodAttribute : Attribute
    {
        public TestMethodAttribute()
        {
        }
    }
    
    // TODO:
    // attribute parameter to run tests in groups
    // results to HTML, count unit tests and create statistics

    class TestUtility
    {
        static void Main(string[] args)
        {
            try
            {
                Object instance;

                Assembly assem = Assembly.GetEntryAssembly();

                Type[] types = assem.GetTypes();

                foreach (Type cls in types)
                {
                    if (cls.GetCustomAttributes(typeof(TestClassAttribute), false).GetLength(0) > 0)
                    {
                        System.Console.WriteLine("\n -- Runnung unit tests from TestClass: " + cls.Name.ToString() + " -- \n");

                        instance = Activator.CreateInstance(cls); ;

                        MemberInfo[] methods = cls.GetMethods();
                        foreach (MemberInfo method in methods)
                        {
                            if (method.GetCustomAttributes(typeof(TestMethodAttribute), false).GetLength(0) > 0)
                            {
                                // call test method
                                System.Console.WriteLine("  |\n  |\n  |->  Runnung unit test: " + cls.Name.ToString() + " :: " + method.Name.ToString());

                                try
                                {
                                    cls.InvokeMember(method.Name.ToString(), BindingFlags.InvokeMethod | BindingFlags.Instance | BindingFlags.Public, null, instance, null);

                                    System.Console.WriteLine("  |\n  |->  Test finished ok!");
                                }
                                catch (TargetInvocationException exception)
                                {
                                    if (exception.InnerException is UnitTestException)
                                        System.Console.WriteLine("  |\n  |->  Test failed: " + ((UnitTestException) exception.InnerException).GetMessage());
                                    else
                                        System.Console.WriteLine("  |\n  |->  Test failed: " + exception.GetBaseException().ToString());
                                }
                            }
                        }
                        System.Console.WriteLine("  |\n\n");
                    }
                }
                System.Console.WriteLine(" Press Enter to confirm the results");
                System.Console.ReadLine();
            }
            catch (System.NullReferenceException)
            {
                Console.WriteLine("TestUtility error");
            }
        }
    }
}
