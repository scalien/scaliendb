using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;

namespace Scalien
{
    public class TestDatabase
    {
        private SqlConnection connection;
        private static string scopedIdentityQuery = "SELECT CAST(SCOPE_IDENTITY() AS bigint)";
        private Int64? testID;
        private bool started;
        private DateTime startDate;
        private string name;
        private string version;
        private int? majorVersion;
        private int? minorVersion;
        private int? releaseVersion;

        public TestDatabase(string connectionString)
        {
            connection = new SqlConnection(connectionString);
            connection.Open();
            started = false;
        }

        ~TestDatabase()
        {
            Close();
        }

        public void Close()
        {
            connection.Close();
        }

        public void StartTest(string name, bool lazy = true)
        {
            started = true;
            this.name = name;
            startDate = DateTime.Now;
            if (!lazy)
                CreateTest();
        }

        public void SetVersion(string version)
        {
            this.version = version;
        }

        public void SetVersion(string version, int majorVersion, int minorVersion, int releaseVersion)
        {
            this.version = version;
            this.majorVersion = majorVersion;
            this.minorVersion = minorVersion;
            this.releaseVersion = releaseVersion;
        }

        private void AddParameter<T>(SqlCommand command, string name, T param)
        {
            if (param == null)
                command.Parameters.Add(new SqlParameter(name, DBNull.Value));
            else
                command.Parameters.Add(new SqlParameter(name, param));
        }

        private void CreateTest()
        {
            var query = @"INSERT INTO Test (StartDate, Name, Version, MajorVersion, MinorVersion, ReleaseVersion) 
                        VALUES (@StartDate, @Name, @Version, @MajorVersion, @MinorVersion, @ReleaseVersion)";
            query += ";" + scopedIdentityQuery;
            using (SqlCommand command = new SqlCommand(query, connection))
            {
                AddParameter(command, "Name", name);
                AddParameter(command, "StartDate", startDate);
                AddParameter(command, "Version", version);
                AddParameter(command, "MajorVersion", majorVersion);
                AddParameter(command, "MinorVersion", minorVersion);
                AddParameter(command, "ReleaseVersion", releaseVersion);
                
                testID = (Int64)command.ExecuteScalar();
            }
        }

        public void StopTest()
        {
            testID = null;
        }

        public Int64 GetCurrentTestID()
        {
            if (testID != null)
                return testID.Value;

            if (started)
            {
                CreateTest();
                return testID.Value;
            }

            var query = "SELECT TOP 1 CAST(TestID AS bigint) FROM Test ORDER BY TestID DESC";
            using (SqlCommand command = new SqlCommand(query, connection))
            {
                testID = (Int64) command.ExecuteScalar();
                return testID.Value;
            }
        }

        public Int64 LogError(ErrorLogEntry error)
        {
            var query = error.Query + ";" + scopedIdentityQuery;
            using (SqlCommand command = new SqlCommand(query, connection))
            {
                foreach (var param in error.Parameters)
                {
                    command.Parameters.Add(param);
                }
                return (Int64)command.ExecuteScalar();
            }
        }
    }
}
