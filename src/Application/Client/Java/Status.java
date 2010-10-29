package com.scalien.scaliendb;

public class Status
{
	public final static int SDBP_SUCCESS		= 0;
	public final static int SDBP_API_ERROR		= -1;

	public final static int SDBP_PARTIAL		= -101;
	public final static int SDBP_FAILURE		= -102;

	public final static int SDBP_NOMASTER		= -201;
	public final static int SDBP_NOCONNECTION	= -202;

	public final static int SDBP_MASTER_TIMEOUT	= -301;
	public final static int SDBP_GLOBAL_TIMEOUT	= -302;
    public final static int SDBP_PRIMARY_TIMEOUT= -303;

	public final static int SDBP_NOSERVICE		= -401;
	public final static int SDBP_FAILED			= -402;
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

