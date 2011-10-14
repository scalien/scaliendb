using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.IO;

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

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public class ExpectedExceptionAttribute : Attribute
    {
        public ExpectedExceptionAttribute(System.Type type, string message)
        {
        }
    }

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public class TestInitializeAttribute : Attribute
    {
        public TestInitializeAttribute()
        {
        }
    }

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public class HostTypeAttribute : Attribute
    {
        public HostTypeAttribute(string name)
        {
        }
    }

    public class TestContext
    {
    }

    // TODO:
    // attribute parameter to run tests in groups
    // results to HTML, count unit tests and create statistics

    class TestClass
    {
        public TestClass()
        {
            expectedExceptions = new List<ExpectedExceptionAttribute>();
        }

        public Type type { get; set; }
        public Object instance { get; set; }
        public List<ExpectedExceptionAttribute> expectedExceptions;
    }

    class TestUtility
    {
        private FileStream fs;
        private StreamWriter sw;

        static void Main(string[] args)
        {
            new TestUtility().RunTests();
        }

        public void WriteLine(string msg)
        {
            System.Console.WriteLine(msg);
            sw.WriteLine(msg);
            sw.Flush();
        }

        public void RunTests()
        {
            try
            {
                int numTests = 0;
                int numSucceeded = 0;

                fs = new FileStream("out.txt", FileMode.Create);
                sw = new StreamWriter(fs);

                Assembly assem = Assembly.GetEntryAssembly();

                Type[] types = assem.GetTypes();

                var testClasses = new List<TestClass>();
                var failedTests = new List<string>();

                // Finding tests
                foreach (Type cls in types)
                {
                    if (cls.GetCustomAttributes(typeof(TestClassAttribute), false).GetLength(0) > 0)
                    {
                        WriteLine("\n -- Loading unit tests from TestClass: " + cls.Name.ToString() + " -- \n");
                        var testClass = new TestClass();
                        testClass.type = cls;
                        //testClass.instance = Activator.CreateInstance(cls);
                        if (cls.Name.ToString() == "FileTableTest")
                            testClasses.Add(testClass);
                        else
                            testClasses.Insert(0, testClass);
                    }
                }

                // Ordering tests by dependencies
                var orderedTestClasses = new List<TestClass>();
                foreach (var testClass in testClasses)
                {
                    testClass.instance = Activator.CreateInstance(testClass.type);
                    orderedTestClasses.Add(testClass);
                }
                testClasses = orderedTestClasses;

                foreach (var testClass in testClasses)
                {
                    WriteLine("\n -- Running unit tests from TestClass: " + testClass.type.Name.ToString() + " -- \n");

                    MemberInfo[] methods = testClass.type.GetMethods();
                    foreach (MemberInfo method in methods)
                    {
                        if (method.GetCustomAttributes(typeof(TestMethodAttribute), false).GetLength(0) > 0)
                        {
                            //if (method.Name != "UnlockTransaction_RecordUpdated")
                            //    continue;

                            numTests += 1;
                            // call test method
                            WriteLine("  |\n  |\n  |->  Running unit test: " + testClass.type.Name.ToString() + " :: " + method.Name.ToString());

                            try
                            {
                                testClass.type.InvokeMember(method.Name.ToString(), BindingFlags.InvokeMethod | BindingFlags.Instance | BindingFlags.Public, null, testClass.instance, null);

                                WriteLine("  |\n  |->  Test finished ok!");
                                numSucceeded += 1;
                            }
                            catch (TargetInvocationException exception)
                            {
                                if (exception.InnerException is UnitTestException)
                                    WriteLine("  |\n  |->  Test failed: " + ((UnitTestException)exception.InnerException).GetMessage());
                                else
                                    WriteLine("  |\n  |->  Test failed: " + exception.GetBaseException().ToString());

                                var name = testClass.type.Name.ToString() + "." + method.Name.ToString();
                                failedTests.Add(name);
                            }
                        }
                        if (method.GetCustomAttributes(typeof(ExpectedExceptionAttribute), false).GetLength(0) > 0)
                        {
                            // TODO: add expected exceptions to the class
                        }
                    }
                    System.Console.WriteLine("  |\n\n");
                }

                WriteLine("\n\n Press Enter to confirm the results");
                WriteLine("" + numSucceeded + " out of " + numTests + " test succeeded.");
                foreach (var failed in failedTests)
                {
                    WriteLine(failed);
                }
                System.Console.ReadLine();
            }
            catch (System.NullReferenceException)
            {
                WriteLine("TestUtility error");
            }
            finally
            {
                if (sw != null)
                    sw.Flush();
                if (fs != null)
                    fs.Close();
            }
        }
    }
}
