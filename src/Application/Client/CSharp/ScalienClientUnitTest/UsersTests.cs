using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Scalien;

namespace ScalienClientUnitTesting
{
    //[TestClass]
    class UsersTests
    {
        [TestMethod]
        public void GetSetSubmitUsers()
        {
            System.Console.WriteLine("TestingGetSetSubmit");

            Users uTest = new Users(Config.controllers);
            uTest.init();
            uTest.resetTables();

            Assert.IsTrue(uTest.testGetSetSubmit());
        }
    }
}
