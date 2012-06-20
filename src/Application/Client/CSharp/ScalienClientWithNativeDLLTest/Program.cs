using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Scalien
{
    class Program
    {
        public static void Main(string[] args)
        {
            NativeLoader.Load();
            //new ScalienClientUnitTesting.SimpleUnitTests().ListTestsWithoutProxies();
            ScalienClientUnitTesting.FailOverTests.minCrashSleepTime = 600;
            ScalienClientUnitTesting.FailOverTests.randomCrashSleepTime = 3000;
            new ScalienClientUnitTesting.FailOverTests().TestRandomCrashServer();
        }
    }
}
