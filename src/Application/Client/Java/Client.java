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
    
    SWIGTYPE_p_void getPtr() {
        return cptr;
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
    
    public long createQuorum(long[] nodes) throws SDBPException {
		SDBP_NodeParams nodeParams = new SDBP_NodeParams(nodes.length);
		for (int i = 0; i < nodes.length; i++) {
			nodeParams.AddNode(Long.toString(nodes[i]));
		}
        
        int status = scaliendb_client.SDBP_CreateQuorum(cptr, nodeParams);
        nodeParams.Close();

		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
        
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return result.getNumber();
    }
    
    public long createDatabase(String name) throws SDBPException {
        int status = scaliendb_client.SDBP_CreateDatabase(cptr, name);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
        
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return result.getNumber();
    }
    
    public long createTable(long databaseID, long quorumID, String name) throws SDBPException {
        BigInteger biDatabaseID = BigInteger.valueOf(databaseID);
        BigInteger biQuorumID = BigInteger.valueOf(quorumID);
        int status = scaliendb_client.SDBP_CreateTable(cptr, biDatabaseID, biQuorumID, name);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
        
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return result.getNumber();
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

	public byte[] getData(byte[] key) throws SDBPException {
		int status = scaliendb_client.SDBP_GetCStr(cptr, key, key.length);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return null;
				
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return result.getValueData();
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

    public int set(byte[] key, byte[] value) throws SDBPException {
		int status = scaliendb_client.SDBP_SetCStr(cptr, key, key.length, value, value.length);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return status;
				
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return status;
	}
	
	public int setIfNotExists(String key, String value) throws SDBPException {
		int status = scaliendb_client.SDBP_SetIfNotExists(cptr, key, value);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return status;
				
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return status;
	}

    public int setIfNotExists(byte[] key, byte[] value) throws SDBPException {
		int status = scaliendb_client.SDBP_SetIfNotExistsCStr(cptr, key, key.length, value, value.length);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return status;
				
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return status;
	}
	
	public int testAndSet(String key, String test, String value) throws SDBPException {
		int status = scaliendb_client.SDBP_TestAndSet(cptr, key, test, value);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return status;
				
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return status;
	}

	public int testAndSet(byte[] key, byte[] test, byte[] value) throws SDBPException {
		int status = scaliendb_client.SDBP_TestAndSetCStr(cptr, key, key.length, test, test.length, value, value.length);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return status;
				
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return status;
	}
	
	public long add(String key, long number) throws SDBPException {
		int status = scaliendb_client.SDBP_Add(cptr, key, number);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return 0;
                    
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return result.getNumber();
	}

	public long add(byte[] key, long number) throws SDBPException {
		int status = scaliendb_client.SDBP_AddCStr(cptr, key, key.length, number);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return 0;
                    
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return result.getNumber();
	}

	public int append(String key, String value) throws SDBPException {
		int status = scaliendb_client.SDBP_Append(cptr, key, value);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return status;
				
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return status;
	}

    public int append(byte[] key, byte[] value) throws SDBPException {
		int status = scaliendb_client.SDBP_AppendCStr(cptr, key, key.length, value, value.length);
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

	public int delete(byte[] key) throws SDBPException {
		int status = scaliendb_client.SDBP_DeleteCStr(cptr, key, key.length);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return status;
		
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return status;
	}
	
	public String remove(String key) throws SDBPException {
		int status = scaliendb_client.SDBP_Remove(cptr, key);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return null;
		
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return result.getValue();
	}

	public String remove(byte[] key) throws SDBPException {
		int status = scaliendb_client.SDBP_RemoveCStr(cptr, key, key.length);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return null;
		
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return result.getValue();
	}
	
    public int begin() {
        return scaliendb_client.SDBP_Begin(cptr);
    }
    
    public int submit() throws SDBPException {
        int status = scaliendb_client.SDBP_Submit(cptr);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            throw new SDBPException(Status.toString(status));
        }
        return status;
    }
    
    public int cancel() {
        return scaliendb_client.SDBP_Cancel(cptr);
    }

	public boolean isBatched() {
		return scaliendb_client.SDBP_IsBatched(cptr);
	}
	
	public static void setTrace(boolean trace) {
		scaliendb_client.SDBP_SetTrace(trace);
	}
	
	public static void main(String[] args) {
		try {
            final String databaseName = "testdb";
            final String tableName = "testtable";
            String[] nodes = {"127.0.0.1:7080"};

            Client.setTrace(true);
            
            Client client = new Client(nodes);
            Database db = new Database(client, databaseName);
            Table table = new Table(client, db, tableName);
            
            table.set("a", "0");
            table.add("a", 10);
            String value = table.get("a");

            System.out.println(value);
		} catch (SDBPException e) {
			System.out.println(e.getMessage());
		}
	}
}