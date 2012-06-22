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
            Scalien.Arguments arguments = new Scalien.Arguments(args);

            var tests = new List<string>();
            if (arguments["f"] != null)
            {
                var filename = arguments["f"];
                using (var file = new System.IO.StreamReader(filename))
                {
                    string line;
                    while ((line = file.ReadLine()) != null)
                    {
                        line = line.Trim();
                        if (line.Length > 0)
                            tests.Add(line);
                    }
                }
            }
            
            if (arguments["t"] != null)
            {
                var test = arguments["t"];
                tests.Add(test);
            }

            int numRuns = 1;
            if (arguments["n"] != null)
            {
                numRuns = Int32.Parse(arguments["n"]);
            }

            new TestUtility().RunTests(tests, numRuns);
        }

        public void WriteLine(string msg)
        {
            System.Console.WriteLine(msg);
            sw.WriteLine(msg);
            sw.Flush();
        }

        public List<TestClass> GetOrderedTestClasses(List<TestClass> testClasses)
        {
            // Ordering tests by dependencies
            var orderedTestClasses = new List<TestClass>();
            foreach (var testClass in testClasses)
            {
                testClass.instance = Activator.CreateInstance(testClass.type);
                orderedTestClasses.Add(testClass);
            }
            return orderedTestClasses;
        }

        public List<TestClass> LoadTestClassesByTypeNames(List<string> tests, Type[] types)
        {
            var testClasses = new List<TestClass>();

            // Finding tests
            foreach (Type cls in types)
            {
                if (cls.GetCustomAttributes(typeof(TestClassAttribute), false).GetLength(0) > 0)
                {
                    var query = from test in tests where test.StartsWith(cls.Name + ".") select test;
                    if (!query.Any())
                        continue;

                    WriteLine("\n -- Loading unit tests from " + cls.Name + " -- \n");
                    var testClass = new TestClass();
                    testClass.type = cls;
                    //testClass.instance = Activator.CreateInstance(cls);
                    if (cls.Name.ToString() == "FileTableTest")
                        testClasses.Add(testClass);
                    else
                        testClasses.Insert(0, testClass);
                }
            }

            return testClasses;
        }

        public void RunTests(List<string> tests, int numRuns)
        {
            try
            {
                int numTests = 0;
                int numSucceeded = 0;

                fs = new FileStream("out.txt", FileMode.Create);
                sw = new StreamWriter(fs);

                Type[] types = Assembly.GetEntryAssembly().GetTypes();

                var failedTests = new List<string>();
                var testClasses = GetOrderedTestClasses(LoadTestClassesByTypeNames(tests, types));

                foreach (var testClass in testClasses)
                {
                    WriteLine("\n -- Running unit tests from  " + testClass.type.Name.ToString() + " -- \n");

                    MemberInfo[] methods = testClass.type.GetMethods();
                    foreach (MemberInfo method in methods)
                    {
                        if (method.GetCustomAttributes(typeof(TestMethodAttribute), false).GetLength(0) > 0)
                        {
                            var testName = testClass.type.Name + "." + method.Name;
                            var query = from test in tests where test.Length <= testName.Length && testName.StartsWith(test) select test;
                            if (!query.Any())
                                continue;

                            for (int i = 0; i < numRuns; i++)
                            {
                                numTests += 1;
                                WriteLine("  |\n  |\n  |->  Running unit test[" + i + "]: " + testClass.type.Name.ToString() + "." + method.Name.ToString());

                                try
                                {
                                    // call test method
                                    testClass.type.InvokeMember(method.Name.ToString(), BindingFlags.InvokeMethod | BindingFlags.Instance | BindingFlags.Public, null, testClass.instance, null);

                                    WriteLine("  |\n  |->  Test finished ok!");
                                    numSucceeded += 1;
                                }
                                catch (TargetInvocationException exception)
                                {
                                    if (exception.InnerException is UnitTestException)
                                        WriteLine("  |\n  |->  Test failed: " + ((UnitTestException)exception.InnerException).GetMessage());
                                    else
                                    {
                                        WriteLine("  |\n  |->  Test failed: " + exception.GetBaseException().ToString());
                                        //throw exception;
                                    }

                                    var name = testClass.type.Name.ToString() + "." + method.Name.ToString();
                                    failedTests.Add(name);
                                    if (numRuns > 0)
                                        break;
                                }
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
