using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;

namespace Scalien
{
    public class ErrorLogEntry
    {
        private List<SqlParameter> parameters;

        public ErrorLogEntry()
        {
            parameters = new List<SqlParameter>();
        }

        public List<SqlParameter> Parameters
        {
            get
            {
                return parameters;
            }
        }

        public string Query
        {
            get
            {
                return @"
                INSERT INTO ErrorLog 
                    (ErrorDate, HostName, ProcessID, IPAddress, FileName, LineNumber, ExceptionType, 
                    ExceptionMessage, ExceptionStackTrace, TestID, CommandLine, ExceptionSource)
                VALUES
                    (GetDate(), @HostName, @ProcessID, @IPAddress, @FileName, @LineNumber, @ExceptionType,
                    @ExceptionMessage, @ExceptionStackTrace, @TestID, @CommandLine, @ExceptionSource)";
            }
        }

        SqlParameter sqlHostName;
        public string HostName
        {
            get
            {
                if (sqlHostName.Value == DBNull.Value)
                    return null;

                return (string)sqlHostName.Value;
            }
            
            set
            {
                sqlHostName = new SqlParameter("HostName", value);
                parameters.Add(sqlHostName);
            }
        }

        SqlParameter sqlProcessID;
        public Int64? ProcessID
        {
            get
            {
                if (sqlHostName.Value == DBNull.Value)
                    return null;

                return (Int64)sqlHostName.Value;
            }

            set
            {
                sqlProcessID = new SqlParameter("ProcessID", value);
                parameters.Add(sqlProcessID);
            }
        }

        // @TODO
        SqlParameter sqlIPAddress;
        public byte[] IPAddress
        {
            get
            {
                if (sqlIPAddress.Value == DBNull.Value)
                    return null;

                return (byte[])sqlIPAddress.Value;
            }

            set
            {
                sqlIPAddress = new SqlParameter("IPAddress", value);
                parameters.Add(sqlIPAddress);
            }
        }

        SqlParameter sqlFileName;
        public string FileName
        {
            get
            {
                if (sqlFileName.Value == DBNull.Value)
                    return null;

                return (string)sqlFileName.Value;
            }

            set
            {
                sqlFileName = new SqlParameter("FileName", value);
                parameters.Add(sqlFileName);
            }
        }

        SqlParameter sqlLineNumber;
        public int? LineNumber
        {
            get
            {
                if (sqlLineNumber.Value == DBNull.Value)
                    return null;

                return (int)sqlLineNumber.Value;
            }

            set
            {
                sqlLineNumber = new SqlParameter("LineNumber", value);
                parameters.Add(sqlLineNumber);
            }
        }

        SqlParameter sqlExceptionType;
        public string ExceptionType
        {
            get
            {
                if (sqlExceptionType.Value == DBNull.Value)
                    return null;

                return (string)sqlExceptionType.Value;
            }

            set
            {
                sqlExceptionType = new SqlParameter("ExceptionType", value);
                parameters.Add(sqlExceptionType);
            }
        }

        SqlParameter sqlExceptionMessage;
        public string ExceptionMessage
        {
            get
            {
                if (sqlExceptionMessage.Value == DBNull.Value)
                    return null;

                return (string)sqlExceptionMessage.Value;
            }

            set
            {
                sqlExceptionMessage = new SqlParameter("ExceptionMessage", value);
                parameters.Add(sqlExceptionMessage);
            }
        }

        SqlParameter sqlExceptionStackTrace;
        public string ExceptionStackTrace
        {
            get
            {
                if (sqlExceptionStackTrace.Value == DBNull.Value)
                    return null;

                return (string)sqlExceptionStackTrace.Value;
            }

            set
            {
                sqlExceptionStackTrace = new SqlParameter("ExceptionStackTrace", value);
                parameters.Add(sqlExceptionStackTrace);
            }
        }

        SqlParameter sqlExceptionSource;
        public string ExceptionSource
        {
            get
            {
                if (sqlExceptionSource.Value == DBNull.Value)
                    return null;

                return (string)sqlExceptionSource.Value;
            }

            set
            {
                sqlExceptionSource = new SqlParameter("ExceptionSource", value);
                parameters.Add(sqlExceptionSource);
            }
        }

        SqlParameter sqlTestID;
        public Int64? TestID
        {
            get
            {
                if (sqlTestID.Value == DBNull.Value)
                    return null;

                return (Int64)sqlTestID.Value;
            }

            set
            {
                sqlTestID = new SqlParameter("TestID", value);
                parameters.Add(sqlTestID);
            }
        }

        SqlParameter sqlCommandLine;
        public string CommandLine
        {
            get
            {
                if (sqlCommandLine.Value == DBNull.Value)
                    return null;

                return (string)sqlCommandLine.Value;
            }

            set
            {
                sqlCommandLine = new SqlParameter("CommandLine", value);
                parameters.Add(sqlCommandLine);
            }
        }
    }
}
