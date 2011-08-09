namespace Scalien
{
    /// <summary>
    /// Collection of ScalienDB client status codes.
    /// </summary>
    /// <seealso cref="SDBPException"/>
    public class Status
    {
        /// <summary>
        /// The operation succeeded.
        /// </summary>
        public const int SDBP_SUCCESS = 0;

        /// <summary>
        /// The operation failed because of a client-side error.
        /// </summary>
        public const int SDBP_API_ERROR = -1;

        /// <summary>
        /// A number of batched operations failed.
        /// </summary>
        public const int SDBP_PARTIAL = -101;

        /// <summary>
        /// The operation failed.
        /// </summary>
        public const int SDBP_FAILURE = -102;

        /// <summary>
        /// No master was found in the ScalienDB cluster.
        /// </summary>
        public const int SDBP_NOMASTER = -201;

        /// <summary>
        /// Connectivity error.
        /// </summary>
        public const int SDBP_NOCONNECTION = -202;

        /// <summary>
        /// No primary was found for one of the quorums.
        /// </summary>
        public const int SDBP_NOPRIMARY = -203;

        /// <summary>
        /// Master timeout occured.
        /// </summary>
        public const int SDBP_MASTER_TIMEOUT = -301;

        /// <summary>
        /// Global timeout occured.
        /// </summary>
        public const int SDBP_GLOBAL_TIMEOUT = -302;

        /// <summary>
        /// Primary timeout occured.
        /// </summary>
        public const int SDBP_PRIMARY_TIMEOUT = -303;

        /// <summary>
        /// The ScalienDB server returned no service.
        /// </summary>
        public const int SDBP_NOSERVICE = -401;
        
        /// <summary>
        /// Key not found.
        /// </summary>
        public const int SDBP_FAILED = -402;

        /// <summary>
        /// There was a schema error.
        /// </summary>
        public const int SDBP_BADSCHEMA = -403;

        /// <summary>
        /// Return a textual presentation of a status code.
        /// </summary>
        /// <param name="status">The status.</param>
        /// <returns>The textual representation of the status code.</returns>
        public static string ToString(int status)
        {
            switch (status)
            {
                case SDBP_SUCCESS:
                    return "SDBP_SUCCESS";
                case SDBP_API_ERROR:
                    return "SDBP_API_ERROR";
                case SDBP_PARTIAL:
                    return "SDBP_PARTIAL";
                case SDBP_FAILURE:
                    return "SDBP_FAILURE";
                case SDBP_NOMASTER:
                    return "SDBP_NOMASTER";
                case SDBP_NOCONNECTION:
                    return "SDBP_NOCONNECTION";
                case SDBP_NOPRIMARY:
                    return "SDBP_NOPRIMARY";
                case SDBP_MASTER_TIMEOUT:
                    return "SDBP_MASTER_TIMEOUT";
                case SDBP_GLOBAL_TIMEOUT:
                    return "SDBP_GLOBAL_TIMEOUT";
                case SDBP_PRIMARY_TIMEOUT:
                    return "SDBP_PRIMARY_TIMEOUT";
                case SDBP_NOSERVICE:
                    return "SDBP_NOSERVICE";
                case SDBP_FAILED:
                    return "SDBP_FAILED";
                case SDBP_BADSCHEMA:
                    return "SDBP_BADSCHEMA";
            }

            return "<UNKNOWN>";
        }
    }
}
