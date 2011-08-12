package com.scalien.scaliendb;

/**
 * Collection of ScalienDB client status codes.
 */
public class Status
{
    /** The operation succeeded. */
    public final static int SDBP_SUCCESS        = 0;
    /** The operation failed because of a client-side error. */
    public final static int SDBP_API_ERROR      = -1;

    /**
     * TRANSPORT STATUS
     */

    /** Only a portion of the commands could be sent before a timeout occured */
    public final static int SDBP_PARTIAL        = -101;
    /** A number of batched operations failed. */
    public final static int SDBP_FAILURE        = -102;

    /** 
     * CONNECTIVITY STATUS
     */
    
    /** No master was found in the ScalienDB cluster. */
    public final static int SDBP_NOMASTER       = -201;
    /** Connectivity error. */
    public final static int SDBP_NOCONNECTION   = -202;
    /** No primary was found for one of the quorums. */
    public final static int SDBP_NOPRIMARY      = -203;

    /**
     * TIMEOUT STATUS
     */
    
    /** Master timeout occured. */
    public final static int SDBP_MASTER_TIMEOUT = -301;
    /** Global timeout occured. */
    public final static int SDBP_GLOBAL_TIMEOUT = -302;
    /** Primary timeout occured. */
    public final static int SDBP_PRIMARY_TIMEOUT= -303;

    /**
     * COMMAND STATUS
     */
    
    /** The ScalienDB server returned no service. */
    public final static int SDBP_NOSERVICE      = -401;
    /** Key not found. */
    public final static int SDBP_FAILED         = -402;
    /** There was a schema error. */
    public final static int SDBP_BADSCHEMA      = -403;
    
    /**
     * 	Return a textual presentation of a status code. 
     */
    public static String toString(int status)
    {
        switch (status) {
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

        return "SDBP_UNKNOWN_STATUS";
    }
}

