package com.scalien.scaliendb;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;

public class Client
{
	static {
		System.loadLibrary("scaliendb_client");
	}

	private SWIGTYPE_p_void cptr;
	private Result result;
    private Result lastResult;
	
    /**
     * Creates client object.
     *
     * @param   nodes   the list addresses of controllers e.g. "localhost:7080"
     *
     */
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
    
    /**
     * Closes the client object.
     *
     * This method may be called to release any resources associated with the client object. It is
     * also called from finalize automatically, but calling this method is deterministic unlike the 
     * call of finalize.
     */
    public void close() {
        if (cptr != null) {
            if (lastResult != result)
                result.close();
            scaliendb_client.SDBP_Destroy(cptr);
            cptr = null;
        }
    }
    
    SWIGTYPE_p_void getPtr() {
        return cptr;
    }

	protected void finalize() {
        close();
	}
	
    /**
     * Sets the global timeout.
     *
     * The global timeout specifies the maximum time that a client call will block your application.
     * The default is 120 seconds.
     *
     * @param   timeout   the global timeout in milliseconds
     * @see     #getGlobalTimeout()     getGlobalTimeout
     */
	public void setGlobalTimeout(long timeout) {
		scaliendb_client.SDBP_SetGlobalTimeout(cptr, BigInteger.valueOf(timeout));
	}
	
    /**
     * Sets the master timeout.
     *
     * The master timeout specifies the maximum time that the client will spend trying to find the
     * master controller node. The default is 21 seconds.
     *
     * @param   timeout   the master timeout in milliseconds
     * @see     #getMasterTimeout()     getMasterTimeout
     */
	public void setMasterTimeout(long timeout) {
		scaliendb_client.SDBP_SetMasterTimeout(cptr, BigInteger.valueOf(timeout));
	}
	
    /**
     * Returns the global timeout.
     *
     * @return  the global timeout in milliseconds
     * @see     #setGlobalTimeout(long)   setGlobalTimeout
     */
	public long getGlobalTimeout() {
		BigInteger bi = scaliendb_client.SDBP_GetGlobalTimeout(cptr);
		return bi.longValue();
	}
	
    /**
     * Returns the master timeout.
     *
     * @return  the master timeout in milliseconds
     * @see     #setMasterTimeout(long)   setMasterTimeout
     */
	public long getMasterTimeout() {
		BigInteger bi = scaliendb_client.SDBP_GetMasterTimeout(cptr);
		return bi.longValue();
	}
    
    /**
     * Returns the specified quorum.
     *
     * @param   quorumID    the ID of the specified quorum
     * @return              the quorum object
     */
    public Quorum getQuorum(long quorumID) throws SDBPException {
        return new Quorum(this, quorumID);
    }
    
    /**
     * Creates a quorum.
     *
     * @param   nodes   an array of node IDs that makes the quorum
     * @return          the ID of the created quorum
     */
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

    /**
     * Deletes a quorum.
     *
     * @param   quorumID    the ID of the quorum to be deleted
     * @return              the status of the operation
     */
    public int deleteQuorum(long quorumID) throws SDBPException {
        BigInteger biQuorumID = BigInteger.valueOf(quorumID);
        int status = scaliendb_client.SDBP_DeleteQuorum(cptr, biQuorumID);
        return status;
    }
    
    /**
     * Adds node to a quorum.
     *
     * @param   quorumID    the ID of the quorum to which the node is added
     * @param   nodeID      the ID of the node to be added
     * @return              the status of the operation
     */
    public int addNode(long quorumID, long nodeID) throws SDBPException {
        BigInteger biQuorumID = BigInteger.valueOf(quorumID);
        BigInteger biNodeID = BigInteger.valueOf(nodeID);
        int status = scaliendb_client.SDBP_AddNode(cptr, biQuorumID, biNodeID);
        return status;
    }

    /**
     * Removes a node from a quorum.
     *
     * @param   quorumID    the ID of the quorum from which the node is removed
     * @param   nodeID      the ID of the node to be removed
     * @return              the status of the operation
     */
    public int removeNode(long quorumID, long nodeID) throws SDBPException {
        BigInteger biQuorumID = BigInteger.valueOf(quorumID);
        BigInteger biNodeID = BigInteger.valueOf(nodeID);
        int status = scaliendb_client.SDBP_RemoveNode(cptr, biQuorumID, biNodeID);
        return status;
    }
    
    /**
     * Activates a node.
     *
     * @param   nodeID  the ID of the node to be activated
     * @return          the status of the operation
     */
    public int activateNode(long nodeID) throws SDBPException {
        int status = scaliendb_client.SDBP_ActivateNode(cptr, BigInteger.valueOf(nodeID));
        return status;
    }
    
    /**
     * Creates a database.
     *
     * @param   name    the name of the database
     * @return          the ID of the created database
     */
    public long createDatabase(String name) throws SDBPException {
        int status = scaliendb_client.SDBP_CreateDatabase(cptr, name);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
        
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return result.getNumber();
    }

    /**
     * Renames a database.
     *
     * @param   databaseID  the ID of the database to be renamed
     * @param   name        the new name of the database
     * @return              the status of the operation
     */
    public int renameDatabase(long databaseID, String name) throws SDBPException {
        BigInteger biDatabaseID = BigInteger.valueOf(databaseID);
        int status = scaliendb_client.SDBP_RenameDatabase(cptr, biDatabaseID, name);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}

		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        return status;
    }

    /**
     * Deletes a database.
     *
     * @param   databaseID  the ID of the database to be deleted
     * @return              the status of the operation
     */
    public long deleteDatabase(long databaseID) throws SDBPException {
        BigInteger biDatabaseID = BigInteger.valueOf(databaseID);
        int status = scaliendb_client.SDBP_DeleteDatabase(cptr, biDatabaseID);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}

		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        return status;
    }
    
    /**
     * Creates a table.
     *
     * @param   databaseID  the ID of the database in which the new table is created
     * @param   quorumID    the ID of the quorum in which the new table is created
     * @param   name        the name of the table
     * @return              the ID of the created table
     */
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

    /**
     * Renames a table.
     *
     * @param   tableID     the ID of the table to be renamed
     * @param   name        the new name of the table
     * @return              the status of the operation
     */
    public long renameTable(long tableID, String name) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_RenameTable(cptr, biTableID, name);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
        
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return status;
    }

    /**
     * Deletes a table.
     *
     * @param   tableID     the ID of the table to be deleted
     * @return              the status of the operation
     */
    public long deleteTable(long tableID) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_DeleteTable(cptr, biTableID);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
        
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return status;
    }

    /**
     * Truncates a table.
     *
     * @param   tableID     the ID of the table to be truncated
     * @return              the status of the operation
     */
    public long truncateTable(long tableID) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_TruncateTable(cptr, biTableID);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
        
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return status;
    }

    /**
     * Use the specified database.
     *
     * @param   name    the name of the database
     */
    public void useDatabase(String name) throws SDBPException {
        int status = scaliendb_client.SDBP_UseDatabase(cptr, name);
        if (status < 0) {
            throw new SDBPException(Status.toString(status));
        }
    }

    /**
     * Returns the specified database.
     *
     * @param   name    the name of the database
     * @return          the specified database
     */    
    public Database getDatabase(String name) throws SDBPException {
        return new Database(this, name);
    }
    
    /**
     * Use the specified table.
     *
     * @param   name    the name of the table
     */
    public void useTable(String name) throws SDBPException {
        int status = scaliendb_client.SDBP_UseTable(cptr, name);
        if (status < 0) {
            throw new SDBPException(Status.toString(status));
        }
    }
	
    /**
     * Returns the result.
     *
     * When the returned result object is no longer used, close() must be called on it.
     *
     * @return  the result object
     * @see     Result#close()
     */
	public Result getResult() {
        lastResult = result;
		return result;
	}
	
    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @return          the value if found
     */
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

    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @return          the value if found
     */
	public byte[] get(byte[] key) throws SDBPException {
		int status = scaliendb_client.SDBP_GetCStr(cptr, key, key.length);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return null;
				
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return result.getValueBytes();
	}
    
    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @param   defval  the default value
     * @return          the value if found, the default value if not found
     */
    public String get(String key, String defval) {
		int status = scaliendb_client.SDBP_Get(cptr, key);
        if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return defval;
        }
        
        if (isBatched())
            return defval;
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        return result.getValue();
    }

    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @param   defval  the default value
     * @return          the value if found, the default value if not found
     */
    public byte[] get(byte[] key, byte[] defval) {
		int status = scaliendb_client.SDBP_GetCStr(cptr, key, key.length);
        if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return defval;
        }
        
        if (isBatched())
            return defval;
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        return result.getValueBytes();
    }
		
    /**
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     * @return          the status of the operation
     */
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

    /**
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     * @return          the status of the operation
     */
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
	
    /**
     * Associates the specified value with the specified key only if it did not exist previously.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     * @return          the status of the operation
     */
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

    /**
     * Associates the specified value with the specified key only if it did not exist previously.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     * @return          the status of the operation
     */
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
	
    /**
     * Associates the specified value with the specified key only if it matches a specified test value.
     * 
     * The testAndSet command conditionally and atomically associates a key => value pair, but only 
     * if the current value matches the user specified test value.
     *
     * @param   key     key with which the specified value is to be associated
     * @param   test    the user specified value that is tested against the old value
     * @param   value   value to be associated with the specified key
     * @return          the status of the operation
     */
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

    /**
     * Associates the specified value with the specified key only if it matches a specified test value.
     * 
     * The testAndSet command conditionally and atomically associates a key => value pair, but only 
     * if the current value matches the user specified test value.
     *
     * @param   key     key with which the specified value is to be associated
     * @param   test    the user specified value that is tested against the old value
     * @param   value   value to be associated with the specified key
     * @return          the status of the operation
     */
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

    /**
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced and returned.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     * @return          the old value
     */
	public String getAndSet(String key, String value) throws SDBPException {
		int status = scaliendb_client.SDBP_GetAndSet(cptr, key, value);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return null;
				
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return result.getValue();
	}

    /**
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced and returned.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     * @return          the old value
     */
	public byte[] getAndSet(byte[] key, byte[] value) throws SDBPException {
		int status = scaliendb_client.SDBP_GetAndSetCStr(cptr, key, key.length, value, value.length);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return null;
				
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return result.getValueBytes();
	}
    
    /**
     * Adds a numeric value to the specified key. The key must contain a numeric value, otherwise
     * an exception is thrown. When the specified number is negative, a substraction will happen.
     *
     * @param   key     key to which the specified number is to be added
     * @param   number  a numeric value
     * @return          the new value
     */
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

    /**
     * Adds a numeric value to the specified key. The key must contain a numeric value, otherwise
     * an exception is thrown. When the specified number is negative, a substraction will happen.
     *
     * @param   key     key to which the specified number is to be added
     * @param   number  a numeric value
     * @return          the new value
     */
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

    /**
     * Appends the specified value to end of the value of the specified key. If the key did not
     * exist, it is created with the specified value.
     *
     * @param   key     key to which the specified value is to be appended
     * @param   value   the specified value that is appended to end of the existing value
     * @return          the status of the operation
     */
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

    /**
     * Appends the specified value to end of the value of the specified key. If the key did not
     * exist, it is created with the specified value.
     *
     * @param   key     key to which the specified value is to be appended
     * @param   value   the specified value that is appended to end of the existing value
     * @return          the status of the operation
     */
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
    
    /**
     * Deletes the specified key.
     *
     * @param   key     key to be deleted
     * @return          the status of the operation
     */
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

    /**
     * Deletes the specified key.
     *
     * @param   key     key to be deleted
     * @return          the status of the operation
     */
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
	
    /**
     * Deletes the specified key and returns the old value.
     *
     * @param   key     key to be deleted
     * @return          the old value
     */
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

    /**
     * Deletes the specified key and returns the old value.
     *
     * @param   key     key to be deleted
     * @return          the old value
     */
	public byte[] remove(byte[] key) throws SDBPException {
		int status = scaliendb_client.SDBP_RemoveCStr(cptr, key, key.length);
		if (status < 0) {
			result = new Result(scaliendb_client.SDBP_GetResult(cptr));
			throw new SDBPException(Status.toString(status));
		}
		
		if (isBatched())
			return null;
		
		result = new Result(scaliendb_client.SDBP_GetResult(cptr));
		return result.getValueBytes();
	}

    /**
     * Returns the specified keys.
     *
     * @param   startKey    listing starts at this key
     * @param   offset      specifies the offset of the first key to return
     * @param   count       specifies the number of keys returned
     * @return              the list of keys
     */
	public List<String> listKeys(String startKey, int offset, int count) throws SDBPException {
		int status = scaliendb_client.SDBP_ListKeys(cptr, startKey, count, offset);
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        if (status < 0)
			throw new SDBPException(Status.toString(status));

        ArrayList<String> keys = new ArrayList<String>();
        for (result.begin(); !result.isEnd(); result.next())
            keys.add(result.getKey());

        return keys;
	}

    /**
     * Returns the specified keys.
     *
     * @param   startKey    listing starts at this key
     * @param   offset      specifies the offset of the first key to return
     * @param   count       specifies the number of keys returned
     * @return              the list of keys
     */
	public List<byte[]> listKeys(byte[] startKey, int offset, int count) throws SDBPException {
		int status = scaliendb_client.SDBP_ListKeysCStr(cptr, startKey, startKey.length, count, offset);
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        if (status < 0)
			throw new SDBPException(Status.toString(status));

        ArrayList<byte[]> keys = new ArrayList<byte[]>();
        for (result.begin(); !result.isEnd(); result.next())
            keys.add(result.getKeyBytes());

        return keys;
	}

    /**
     * Returns the specified key-value pairs.
     *
     * @param   startKey    listing starts at this key
     * @param   offset      specifies the offset of the first key to return
     * @param   count       specifies the number of keys returned
     * @return              the list of key-value pairs
     */
    public Map<String, String> listKeyValues(String startKey, int offset, int count) throws SDBPException {
		int status = scaliendb_client.SDBP_ListKeyValues(cptr, startKey, count, offset);
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        if (status < 0)
			throw new SDBPException(Status.toString(status));
            
        TreeMap<String, String> keyValues = new TreeMap<String, String>();
        for (result.begin(); !result.isEnd(); result.next())
            keyValues.put(result.getKey(), result.getValue());
        
        return keyValues;
    }

    /**
     * Returns the specified key-value pairs.
     *
     * @param   startKey    listing starts at this key
     * @param   offset      specifies the offset of the first key to return
     * @param   count       specifies the number of keys returned
     * @return              the list of key-value pairs
     */
    public Map<byte[], byte[]> listKeyValues(byte[] startKey, int offset, int count) throws SDBPException {
		int status = scaliendb_client.SDBP_ListKeyValuesCStr(cptr, startKey, startKey.length, count, offset);
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        if (status < 0)
			throw new SDBPException(Status.toString(status));
            
        TreeMap<byte[], byte[]> keyValues = new TreeMap<byte[], byte[]>();
        for (result.begin(); !result.isEnd(); result.next())
            keyValues.put(result.getKeyBytes(), result.getValueBytes());
        
        return keyValues;
    }

    /**
     * Returns the number of key-value pairs.
     *
     * @param   startKey    counting starts at this key
     * @param   offset      specifies the offset of the first key to return
     * @param   count       specifies the maximum number of keys returned
     * @return              the number of key-value pairs
     */
    public long count(String startKey, int offset, int count) throws SDBPException {
		int status = scaliendb_client.SDBP_Count(cptr, startKey, count, offset);
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        if (status < 0)
			throw new SDBPException(Status.toString(status));
        
		return result.getNumber();
    }
    
    /**
     * Returns the number of key-value pairs.
     *
     * @param   startKey    counting starts at this key
     * @param   offset      specifies the offset of the first key to return
     * @param   count       specifies the maximum number of keys returned
     * @return              the number of key-value pairs
     */
    public long count(byte[] startKey, int offset, int count) throws SDBPException {
		int status = scaliendb_client.SDBP_CountCStr(cptr, startKey, startKey.length, count, offset);
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        if (status < 0)
			throw new SDBPException(Status.toString(status));
        
		return result.getNumber();
    }

    /**
     * Begins a batch operation. After begin is called, each command will be batched and
     * submitted or cancelled together.
     *
     * @return              the status of the operation
     * @see     #submit()   submit
     * @see     #cancel()   cancel
     */
    public int begin() {
        if (lastResult != result) {
            result.close();
        }
        result = null;
        lastResult = null;
        return scaliendb_client.SDBP_Begin(cptr);
    }
    
    /**
     * Submits a batch operation.
     *
     * @return              the status of the operation
     * @see     #begin()    begin
     * @see     #cancel()   cancel
     */
    public int submit() throws SDBPException {
        int status = scaliendb_client.SDBP_Submit(cptr);
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        if (status < 0) {
            throw new SDBPException(Status.toString(status));
        }
        return status;
    }
    
    /**
     * Cancels a batch operation.
     *
     * @return              the status of the operation
     * @see     #begin()    begin
     * @see     #submit()   submit
     */
    public int cancel() {
        return scaliendb_client.SDBP_Cancel(cptr);
    }

    /**
     * Returns if the client is batched mode or not.
     *
     * @return              true if batched
     */
	public boolean isBatched() {
		return scaliendb_client.SDBP_IsBatched(cptr);
	}
	
    /**
     * Turns on or off the debug trace functionality.
     */
	public static void setTrace(boolean trace) {
		scaliendb_client.SDBP_SetTrace(trace);
	}
	
    /**
     * Test program entry.
     */
	static void main(String[] args) {
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