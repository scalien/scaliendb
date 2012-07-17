using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace Scalien
{
    public class ErrorLogger
    {
        private TestDatabase database;

        public ErrorLogger(string connectionString)
        {
            try
            {
                database = new TestDatabase(connectionString);
            }
            catch (Exception e)
            {
                // ignore for now
            }
        }

        public ErrorLogger(string connectionString, string name)
        {
            try
            {
                database = new TestDatabase(connectionString);
                database.StartTest(name);
            }
            catch (Exception e)
            {
                // ignore for now
            }
        }

        public void Close()
        {
            if (database == null)
                return;
            
            database.Close();
            database = null;
        }

        public void LogException(Exception exception)
        {
            if (database == null)
                return;

            ErrorLogEntry error = new ErrorLogEntry();

            var exceptionStackTrace = new ExceptionStackTrace(exception);

            error.CommandLine = Environment.CommandLine;
            error.ExceptionMessage = exceptionStackTrace.Message;
            error.ExceptionType = exception.GetType().FullName;
            error.ExceptionSource = exceptionStackTrace.Source;
            error.ExceptionStackTrace = exceptionStackTrace.StackTrace;
            error.FileName = exceptionStackTrace.FileName;
            error.HostName = System.Environment.MachineName;
            error.IPAddress = Utils.GetLocalIP().GetAddressBytes();
            error.LineNumber = exceptionStackTrace.LineNumber;
            error.ProcessID = Process.GetCurrentProcess().Id;
            error.TestID = database.GetCurrentTestID();

            database.LogError(error);
        }
    }
}
