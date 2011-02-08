package com.scalien.scaliendb;

public class Status
{
	public final static int SDBP_SUCCESS		= 0;
	public final static int SDBP_API_ERROR		= -1;

    // TRANSPORT STATUS

    // only a portion of the commands could be sent before a timeout occured
	public final static int SDBP_PARTIAL		= -101;
    // no commands could be sent
	public final static int SDBP_FAILURE		= -102;

    // CONNECTIVITY STATUS
    
    // some controllers were reachable, but there was no master or it went down
	public final static int SDBP_NOMASTER		= -201;
    // the entire grid was unreachable within timeouts
	public final static int SDBP_NOCONNECTION	= -202;
    // the primary could not be found
    public final static int SDBP_NOPRIMARY      = -203;

    // TIMEOUT STATUS
    
    // the master could not be found within the master timeout
	public final static int SDBP_MASTER_TIMEOUT	= -301;
    // the blocking client library call returned because the global timeout has expired
	public final static int SDBP_GLOBAL_TIMEOUT	= -302;
    // the primary could not be found within the primary timeout
    public final static int SDBP_PRIMARY_TIMEOUT= -303;

    // COMMAND STATUS
	
    // the command was not executed
    public final static int SDBP_NOSERVICE		= -401;
    // the command was executed, but its return value was FAILED
	public final static int SDBP_FAILED			= -402;
    // the command was not executed, because of bad schema specification
    public final static int SDBP_BADSCHEMA      = -403;
	
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

