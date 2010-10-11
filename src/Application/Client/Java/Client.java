package com.scalien.scaliendb;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;


public class Client
{
	static {
		System.loadLibrary("scaliendb_client");
	}

	private SWIGTYPE_p_void cptr;
	private Result result;
	
	public Client(String[] nodes) {
		cptr = scaliendb_client.SDBP_Create();
		result = null;
		
		SDBP_NodeParams nodeParams = new SDBP_NodeParams(nodes.length);
		for (int i = 0; i < nodes.length; i++) {
			nodeParams.AddNode(nodes[i]);
		}
		
		int status = scaliendb_client.SDBP_Init(cptr, nodeParams);
		nodeParams.Close();
	}

	public void finalize() {
		scaliendb_client.SDBP_Destroy(cptr);
	}
	
	public void setGlobalTimeout(long timeout) {
		scaliendb_client.SDBP_SetGlobalTimeout(cptr, BigInteger.valueOf(timeout));
	}
	
	public void setMasterTimeout(long timeout) {
		scaliendb_client.SDBP_SetMasterTimeout(cptr, BigInteger.valueOf(timeout));
	}
	
	public long getGlobalTimeout() {
		BigInteger bi = scaliendb_client.SDBP_GetGlobalTimeout(cptr);
		return bi.longValue();
	}
	
	public long getMasterTimeout() {
		BigInteger bi = scaliendb_client.SDBP_GetMasterTimeout(cptr);
		return bi.longValue();
	}

    public void useDatabase(String name) throws SDBPException {
        int status = scaliendb_client.SDBP_UseDatabase(cptr, name);
        if (status < 0) {
            throw new SDBPException(Status.toString(status));
        }
    }
    
    public void useTable(String name) throws SDBPException {
        int status = scaliendb_client.SDBP_UseTable(cptr, name);
        if (status < 0) {
            throw new SDBPException(Status.toString(status));
        }
    }
	
	public Result getResult() {
		return result;
	}
	
	public String get(String key) throws SDBPException {
		int status = scaliendb_client.SDBP_Get(cptr, key);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return null;
				
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return result.getValue();
	}
		
	public int set(String key, String value) throws SDBPException {
		int status = scaliendb_client.SDBP_Set(cptr, key, value);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return status;
				
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return status;
	}
	
	public int delete(String key) throws SDBPException {
		int status = scaliendb_client.SDBP_Delete(cptr, key);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return status;
		
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return status;
	}

	private boolean isBatched() {
		return scaliendb_client.SDBP_IsBatched(cptr);
	}
	
	public static void setTrace(boolean trace) {
		scaliendb_client.SDBP_SetTrace(trace);
	}
	
	public static void main(String[] args) {
		try {
			String[] nodes = {"127.0.0.1:7080"};
			//setTrace(true);
			Client ks = new Client(nodes);
            ks.useDatabase("mediafilter");
            ks.useTable("users");
			String hol = ks.get("hol");
			System.out.println(hol);
			
//			ArrayList<String> keys = ks.listKeys("", "", 0, false, true);
//			for (String key : keys)
//				System.out.println(key);
//			
//			Map<String, String> keyvals = ks.listKeyValues("", "", 0, false, true);
//			Collection c = keyvals.keySet();
//			Iterator it = c.iterator();
//			while (it.hasNext()) {
//				Object key = it.next();
//				String value = keyvals.get(key);
//				System.out.println(key + " => " + value);
//			}
//			
//			long cnt = ks.count(new ListParams().setCount(100));
//			System.out.println(cnt);
		} catch (SDBPException e) {
			System.out.println(e.getMessage());
		}
	}
}