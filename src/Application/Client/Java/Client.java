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

    public static final int CONSISTENCY_ANY     = 0;
    public static final int CONSISTENCY_RYW     = 1;
    public static final int CONSISTENCY_STRICT  = 2;

    public static final int BATCH_DEFAULT       = 0;
    public static final int BATCH_NOAUTOSUBMIT  = 1;
    public static final int BATCH_SINGLE        = 2;
    
    /**
     * Creates client object.
     *
     * @param   nodes   the addresses of controllers, e.g. "localhost:7080"
     *
     */
    public Client(String... nodes) throws SDBPException {
        cptr = scaliendb_client.SDBP_Create();
        result = null;
        
        SDBP_NodeParams nodeParams = new SDBP_NodeParams(nodes.length);
        for (String node : nodes) {
            nodeParams.AddNode(node);
        }
        
        int status = scaliendb_client.SDBP_Init(cptr, nodeParams);
        checkStatus(status);
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
     * @param   timeout                 the global timeout in milliseconds
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
     * @param   timeout                 the master timeout in milliseconds
     * @see     #getMasterTimeout()     getMasterTimeout
     */
    public void setMasterTimeout(long timeout) {
        scaliendb_client.SDBP_SetMasterTimeout(cptr, BigInteger.valueOf(timeout));
    }
    
    /**
     * Returns the global timeout.
     *
     * @return                              the global timeout in milliseconds
     * @see     #setGlobalTimeout(long)     setGlobalTimeout
     */
    public long getGlobalTimeout() {
        BigInteger bi = scaliendb_client.SDBP_GetGlobalTimeout(cptr);
        return bi.longValue();
    }
    
    /**
     * Returns the master timeout.
     *
     * @return                              the master timeout in milliseconds
     * @see     #setMasterTimeout(long)     setMasterTimeout
     */
    public long getMasterTimeout() {
        BigInteger bi = scaliendb_client.SDBP_GetMasterTimeout(cptr);
        return bi.longValue();
    }
    
    /**
     * Sets the consistency level for read operations.
     *
     * <p>There are separate consistency levels the client can operate at.
     *
     * <li>CONSISTENCY_ANY means that any shard server can serve the read requests. Usually this
     * can be used for load-balancing between replicas, and due to total replication it will be
     * consistent most of the time, with a small chance of inconsistency when one of
     * the shard servers are failing.
     *
     * <li>CONSISTENCY_RYW means "read your writes" consistency, clients will always get a
     * consistent view of their own writes.
     *
     * <li>CONSISTENCY_STRICT means that all read requests will be served by the primary shard server.
     * Therefore it is always consistent.
     *
     * @param   consistencyLevel    can be CONSISTENCY_ANY, CONSISTENCY_RYW or CONSISTENCY_STRICT
     */
    public void setConsistencyLevel(int consistencyLevel) {
        scaliendb_client.SDBP_SetConsistencyLevel(cptr, consistencyLevel);
    }

    /**
     * Sets the batch mode for write operations.
     *
     * <p>This determines when the client library actually sends commands to the server.
     *    Note that calling Submit() always sends the currently batched commands, irrespective
     *    of the batch mode.
     *
     * <li>BATCH_DEAULT means that commands are sent when the batch limit (default 1MB) is reached.
     *
     * <li>BATCH_NOAUTOSUBMIT means that commands are never send automatically, only when Submit()
     *     is called explicitly. Once the batch limit is reached an exception is thrown.
     *
     * <li>BATCH_SINGLE means that commands are sent right after they are issued. Since the client
     *     library waits for the command to complete and return, this can be slow.
     *
     * @param   batchMode   can be BATCH_DEFAULT, BATCH_NOAUTOSUBMIT and BATCH_SINGLE
     */    
    public void setBatchMode(int batchMode) {
        scaliendb_client.SDBP_SetBatchMode(cptr, batchMode);
    }
    
    /**
     * Sets the batch limit.
     *
     * The batch limit is the maximum allocated memory the client will allocate for batched
     * requests. If the requests exceed that limit, the client will throw an exception with
     * SDBP_API_ERROR.
     *
     * @param   limit   the maximum allocated memory
     */
    public void setBatchLimit(long limit) {
        scaliendb_client.SDBP_SetBatchLimit(cptr, limit);
    }

    /**
     * Returns the config state in a JSON-serialized string.
     */
    public String getJSONConfigState() {
        return scaliendb_client.SDBP_GetJSONConfigState(cptr);
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
     * Returns all quorums.
     *
     * @return              a list of quorum objects
     */
    public List<Quorum> getQuorums() throws SDBPException {
        long numQuorums = scaliendb_client.SDBP_GetNumQuorums(cptr);
        ArrayList<Quorum> quorums = new ArrayList<Quorum>();
        for (long i = 0; i < numQuorums; i++) {
            BigInteger bi = scaliendb_client.SDBP_GetQuorumIDAt(cptr, i);
            long quorumID = bi.longValue();
            quorums.add(new Quorum(this, quorumID));
        }
        return quorums;
    }
    
    /**
     * Creates a quorum with a default name.
     *
     * @param   nodes   an array of node IDs that makes the quorum
     * @return          the ID of the created quorum
     */
    public Quorum createQuorum(long[] nodes) throws SDBPException {
        SDBP_NodeParams nodeParams = new SDBP_NodeParams(nodes.length);
        for (int i = 0; i < nodes.length; i++) {
            nodeParams.AddNode(Long.toString(nodes[i]));
        }
        
        int status = scaliendb_client.SDBP_CreateQuorum(cptr, "", nodeParams);
        nodeParams.Close();

        checkResultStatus(status, "Cannot create quorum");
        return new Quorum(this, result.getNumber());
    }

    /**
     * Creates a quorum.
     *
     * @param   nodes   an array of node IDs that makes the quorum
     * @param   name    name of quorum
     * @return          the ID of the created quorum
     */
    public Quorum createQuorum(String name, long[] nodes) throws SDBPException {
        SDBP_NodeParams nodeParams = new SDBP_NodeParams(nodes.length);
        for (int i = 0; i < nodes.length; i++) {
            nodeParams.AddNode(Long.toString(nodes[i]));
        }
        
        int status = scaliendb_client.SDBP_CreateQuorum(cptr, name, nodeParams);
        nodeParams.Close();

        checkResultStatus(status, "Cannot create quorum");
        return new Quorum(this, result.getNumber());
    }

    /**
     * Renames a quorum.
     *
     * @param   quorumID    the ID of the quorum to be renamed
     * @param   name    name of quorum
     */
    public void renameQuorum(long quorumID, String name) throws SDBPException {
        BigInteger biQuorumID = BigInteger.valueOf(quorumID);
        int status = scaliendb_client.SDBP_RenameQuorum(cptr, biQuorumID, name);
        checkResultStatus(status, "Cannot rename quorum");
    }

    /**
     * Deletes a quorum.
     *
     * @param   quorumID    the ID of the quorum to be deleted
     */
    public void deleteQuorum(long quorumID) throws SDBPException {
        BigInteger biQuorumID = BigInteger.valueOf(quorumID);
        int status = scaliendb_client.SDBP_DeleteQuorum(cptr, biQuorumID);
        checkResultStatus(status, "Cannot delete quorum");
    }
    
    /**
     * Adds node to a quorum.
     *
     * @param   quorumID    the ID of the quorum to which the node is added
     * @param   nodeID      the ID of the node to be added
     */
    public void addNode(long quorumID, long nodeID) throws SDBPException {
        BigInteger biQuorumID = BigInteger.valueOf(quorumID);
        BigInteger biNodeID = BigInteger.valueOf(nodeID);
        int status = scaliendb_client.SDBP_AddNode(cptr, biQuorumID, biNodeID);
        checkResultStatus(status, "Cannot add node");
    }

    /**
     * Removes a node from a quorum.
     *
     * @param   quorumID    the ID of the quorum from which the node is removed
     * @param   nodeID      the ID of the node to be removed
     */
    public void removeNode(long quorumID, long nodeID) throws SDBPException {
        BigInteger biQuorumID = BigInteger.valueOf(quorumID);
        BigInteger biNodeID = BigInteger.valueOf(nodeID);
        int status = scaliendb_client.SDBP_RemoveNode(cptr, biQuorumID, biNodeID);
        checkStatus(status);
    }
    
    /**
     * Activates a node.
     *
     * @param   nodeID  the ID of the node to be activated
     */
    public void activateNode(long nodeID) throws SDBPException {
        int status = scaliendb_client.SDBP_ActivateNode(cptr, BigInteger.valueOf(nodeID));
        checkStatus(status);
    }
    
    /**
     * Creates a database.
     *
     * @param   name    the name of the database
     * @return          the database object
     */
    public Database createDatabase(String name) throws SDBPException {
        int status = scaliendb_client.SDBP_CreateDatabase(cptr, name);
        checkResultStatus(status);
        return new Database(this, name);
    }

    /**
     * Renames a database.
     *
     * @param   databaseID  the ID of the database to be renamed
     * @param   name        the new name of the database
     */
    public void renameDatabase(long databaseID, String name) throws SDBPException {
        BigInteger biDatabaseID = BigInteger.valueOf(databaseID);
        int status = scaliendb_client.SDBP_RenameDatabase(cptr, biDatabaseID, name);
        checkResultStatus(status);    
    }

    /**
     * Deletes a database.
     *
     * @param   databaseID  the ID of the database to be deleted
     */
    public void deleteDatabase(long databaseID) throws SDBPException {
        BigInteger biDatabaseID = BigInteger.valueOf(databaseID);
        int status = scaliendb_client.SDBP_DeleteDatabase(cptr, biDatabaseID);
        checkResultStatus(status);    
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
        checkResultStatus(status);    
        return result.getNumber();
    }

    /**
     * Renames a table.
     *
     * @param   tableID     the ID of the table to be renamed
     * @param   name        the new name of the table
     */
    public void renameTable(long tableID, String name) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_RenameTable(cptr, biTableID, name);
        checkResultStatus(status);    
    }

    /**
     * Deletes a table.
     *
     * @param   tableID     the ID of the table to be deleted
     */
    public void deleteTable(long tableID) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_DeleteTable(cptr, biTableID);
        checkResultStatus(status);    
    }

    /**
     * Truncates a table.
     *
     * @param   tableID     the ID of the table to be truncated
     */
    public void truncateTable(long tableID) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_TruncateTable(cptr, biTableID);
        checkResultStatus(status);    
    }

    /**
     * Use the specified database.
     *
     * @param   name    the name of the database
     */
    public void useDatabase(String name) throws SDBPException {
        int status = scaliendb_client.SDBP_UseDatabase(cptr, name);
        if (status < 0) {
            if (status == Status.SDBP_NOSERVICE)
                throw new SDBPException(status, "Cannot connect to controller");
            throw new SDBPException(Status.SDBP_BADSCHEMA, "No database found with name '" + name + "'");
        }
    }

    /**
     * Use the specified database.
     *
     * @param   databaseID    the id of the database
     */
    public void useDatabase(long databaseID) throws SDBPException {
        int status = scaliendb_client.SDBP_UseDatabaseID(cptr, BigInteger.valueOf(databaseID));
        if (status < 0) {
            if (status == Status.SDBP_NOSERVICE)
                throw new SDBPException(status, "Cannot connect to controller");
            throw new SDBPException(Status.SDBP_BADSCHEMA, "No database found with id '" + databaseID + "'");
        }
    }

    /**
     * Returns all databases.
     *
     * @return              a list of database objects
     */
    public List<Database> getDatabases() throws SDBPException {
        long numDatabases = scaliendb_client.SDBP_GetNumDatabases(cptr);
        ArrayList<Database> databases = new ArrayList<Database>();
        for (long i = 0; i < numDatabases; i++) {
            String name = scaliendb_client.SDBP_GetDatabaseNameAt(cptr, i);
            databases.add(new Database(this, name));
        }
        return databases;
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
        if (status < 0)
            throw new SDBPException(Status.SDBP_BADSCHEMA, "No table found with name '" + name + "'");
    }

    /**
     * Use the specified table.
     *
     * @param   tableID    the id of the table
     */
    public void useTable(long tableID) throws SDBPException {
        int status = scaliendb_client.SDBP_UseTableID(cptr, BigInteger.valueOf(tableID));
        if (status < 0)
            throw new SDBPException(Status.SDBP_BADSCHEMA, "No table found with id '" + tableID + "'");
    }
    
    /**
     * Returns the result.
     *
     * When the returned result object is no longer used, close() must be called on it.
     *
     * @return  the result object
     * @see     com.scalien.scaliendb.Result#close()
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
            checkStatus(status);
        }
                        
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
            checkStatus(status);
        }
        
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
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        return result.getValueBytes();
    }

    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @return          the value if found
     */
    public long getLong(String key) throws SDBPException {
        String s = get(key);
        if (s == null)
            return 0;
        return Long.parseLong(s);
    }

    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @return          the value if found
     */
    public long getLong(byte[] key) throws SDBPException {
        byte[] r = get(key);
        if (r == null)
            return 0;
        return Long.parseLong(new String(r));
    }

    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @param   defval  the default value
     * @return          the value if found
     */
    public long getLong(String key, long defval) throws SDBPException {
        String s = get(key);
        if (s == null)
            return defval;
        return Long.parseLong(s);
    }

    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @param   defval  the default value
     * @return          the value if found
     */
    public long getLong(byte[] key, long defval) throws SDBPException {
        byte[] r = get(key);
        if (r == null)
            return defval;
        return Long.parseLong(new String(r));
    }

    /**
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     */
    public void set(String key, String value) throws SDBPException {
        int status = scaliendb_client.SDBP_Set(cptr, key, value);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            checkStatus(status);
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
    }

    /**
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     */
    public void set(byte[] key, byte[] value) throws SDBPException {
        int status = scaliendb_client.SDBP_SetCStr(cptr, key, key.length, value, value.length);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            checkStatus(status);
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
    }

    /**
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     */
    public void setLong(String key, long value) throws SDBPException {
        set(key, Long.toString(value));
    }

    /**
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     */
    public void setLong(byte[] key, long value) throws SDBPException {
        set(key, Long.toString(value).getBytes());
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
            checkStatus(status);
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        return result.getSignedNumber();
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
            checkStatus(status);
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        return result.getSignedNumber();
    }
        
    /**
     * Deletes the specified key.
     *
     * @param   key     key to be deleted
     */
    public void delete(String key) throws SDBPException {
        int status = scaliendb_client.SDBP_Delete(cptr, key);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            checkStatus(status);
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
    }

    /**
     * Deletes the specified key.
     *
     * @param   key     key to be deleted
     */
    public void delete(byte[] key) throws SDBPException {
        int status = scaliendb_client.SDBP_DeleteCStr(cptr, key, key.length);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            checkStatus(status);
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
    }
    
    /**
     * Returns the specified keys.
     *
     * @param   startKey    listing starts at this key
     * @param   endKey      listing ends at this key
     * @param   prefix      list only those keys that starts with prefix
     * @param   count       specifies the number of keys returned
     * @return              the list of keys
     */
    public List<String> listKeys(String startKey, String endKey, String prefix, int count, boolean skip)
    throws SDBPException {
        int status = scaliendb_client.SDBP_ListKeys(cptr, startKey, endKey, prefix, count, skip);
        checkResultStatus(status);

        ArrayList<String> keys = new ArrayList<String>();
        for (result.begin(); !result.isEnd(); result.next())
            keys.add(result.getKey());

        return keys;
    }

    /**
     * Returns the specified keys.
     *
     * @param   startKey    listing starts at this key
     * @param   endKey      listing ends at this key
     * @param   prefix      list only those keys that starts with prefix
     * @param   count       specifies the number of keys returned
     * @return              the list of keys
     */
    public List<byte[]> listKeys(byte[] startKey, byte[] endKey, byte[] prefix, int count, boolean skip)
    throws SDBPException {
        int status = scaliendb_client.SDBP_ListKeysCStr(cptr, startKey, startKey.length, 
        endKey, endKey.length, prefix, prefix.length, count, skip);
        checkResultStatus(status);

        ArrayList<byte[]> keys = new ArrayList<byte[]>();
        for (result.begin(); !result.isEnd(); result.next())
            keys.add(result.getKeyBytes());

        return keys;
    }

    /**
     * Returns the specified key-value pairs.
     *
     * @param   startKey    listing starts at this key
     * @param   endKey      listing ends at this key
     * @param   prefix      list only those keys that starts with prefix
     * @param   count       specifies the number of keys returned
     * @return              the list of key-value pairs
     */
    public Map<String, String> listKeyValues(String startKey, String endKey, String prefix, int count, boolean skip)
    throws SDBPException {
        int status = scaliendb_client.SDBP_ListKeyValues(cptr, startKey, endKey, prefix, count, skip);
        checkResultStatus(status);
            
        TreeMap<String, String> keyValues = new TreeMap<String, String>();
        for (result.begin(); !result.isEnd(); result.next())
            keyValues.put(result.getKey(), result.getValue());
        
        return keyValues;
    }

    /**
     * Returns the specified key-value pairs.
     *
     * @param   startKey    listing starts at this key
     * @param   endKey      listing ends at this key
     * @param   prefix      list only those keys that starts with prefix
     * @param   count       specifies the number of keys returned
     * @return              the list of key-value pairs
     */
    public Map<byte[], byte[]> listKeyValues(byte[] startKey, byte[] endKey, byte[] prefix, int count, boolean skip) throws SDBPException {
        int status = scaliendb_client.SDBP_ListKeyValuesCStr(cptr, startKey, startKey.length, endKey, endKey.length, prefix, prefix.length, count, skip);
        checkResultStatus(status);
            
        TreeMap<byte[], byte[]> keyValues = new TreeMap<byte[], byte[]>(new ByteArrayComparator());
        for (result.begin(); !result.isEnd(); result.next())
            keyValues.put(result.getKeyBytes(), result.getValueBytes());
        return keyValues;
    }

    /**
     * Returns the number of key-value pairs.
     *
     * @param   startKey    counting starts at this key
     * @param   endKey      counting ends at this key
     * @param   prefix      count only those keys that starts with prefix
     * @return              the number of key-value pairs
     */
    public long count(String startKey, String endKey, String prefix) throws SDBPException {
        int status = scaliendb_client.SDBP_Count(cptr, startKey, endKey, prefix);
        checkResultStatus(status);        
        return result.getNumber();
    }
    
    /**
     * Returns the number of key-value pairs.
     *
     * @param   startKey    counting starts at this key
     * @param   endKey      counting ends at this key
     * @param   prefix      count only those keys that starts with prefix
     * @return              the number of key-value pairs
     */
    public long count(byte[] startKey, byte[] endKey, byte[] prefix) throws SDBPException {
        int status = scaliendb_client.SDBP_CountCStr(cptr, startKey, startKey.length, endKey, endKey.length, prefix, prefix.length);
        checkResultStatus(status);
        return result.getNumber();
    }
    
    /**
     * Returns a key iterator over keys.
     *
     * @param   ps          the iterations params, usage: (new StringIterParams()).prefix("foo").startKey("bar").endKey("acme")
     */
    public StringKeyIterator getKeyIterator(StringIterParams ps) throws SDBPException {
        return new StringKeyIterator(this, ps);
    }

    /**
     * Returns a key iterator over keys.
     *
     * @param   ps          the iterations params, usage: (new ByteIterParams()).prefix("foo").startKey("bar").endKey("acme")
     */
    public ByteKeyIterator getKeyIterator(ByteIterParams ps) throws SDBPException {
        return new ByteKeyIterator(this, ps);
    }

    /**
     * Returns a key-value iterator over keys.
     *
     * @param   ps          the iterations params, usage: (new StringIterParams()).prefix("foo").startKey("bar").endKey("acme")
     */
    public StringKeyValueIterator getKeyValueIterator(StringIterParams ps) throws SDBPException {
        return new StringKeyValueIterator(this, ps);
    }

    /**
     * Returns a key-value iterator over keys.
     *
     * @param   ps          the iterations params, usage: (new ByteIterParams()).prefix("foo").startKey("bar").endKey("acme")
     */
    public ByteKeyValueIterator getKeyValueIterator(ByteIterParams ps) throws SDBPException {
        return new ByteKeyValueIterator(this, ps);
    }

    /**
     * Begins a batch operation. After begin is called, each command will be batched and
     * submitted or cancelled together.
     *
     * @see     #submit()   submit
     * @see     #cancel()   cancel
     */
    public void begin() throws SDBPException {
        if (lastResult != result) {
            result.close();
        }
        result = null;
        lastResult = null;
        int status = scaliendb_client.SDBP_Begin(cptr);
        checkStatus(status);
    }
    
    /**
     * Submits a batch operation.
     *
     * @see     #begin()    begin
     * @see     #cancel()   cancel
     */
    public void submit() throws SDBPException {
        int status = scaliendb_client.SDBP_Submit(cptr);
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        checkStatus(status);
    }
    
    /**
     * Cancels a batch operation.
     *
     * @see     #begin()    begin
     * @see     #submit()   submit
     */
    public void cancel() throws SDBPException {
        int status = scaliendb_client.SDBP_Cancel(cptr);
        checkStatus(status);        
    }

    /**
     * Turns on or off the debug trace functionality.
     */
    public static void setTrace(boolean trace) {
        scaliendb_client.SDBP_SetTrace(trace);
    }

    /**
     * Checks the status of the operation and saves the result. Throws exception with the name of 
     * the status in case of an error.
     *
     * @param   status      the status of the operation
     */
    private void checkResultStatus(int status) throws SDBPException {
        checkResultStatus(status, null);
    }

    /**
     * Checks the status of the operation and saves the result. Throws exception with the name of 
     * the status in case of an error.
     *
     * @param   status      the status of the operation
     * @param   msg         the default message given to the exception
     */
    private void checkResultStatus(int status, String msg) throws SDBPException {
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        checkStatus(status, msg);
    }
    
    /**
     * Checks the status of the operation. Throws exception with the name of the status in case of
     * an error.
     *
     * @param   status      the status of the operation
     */
    private void checkStatus(int status) throws SDBPException {
        checkStatus(status, null);
    }

    /**
     * Checks the status of the operation. Throws exception with the name of the status in case of
     * an error.
     *
     * @param   status      the status of the operation
     * @param   msg         the default message given to the exception
     */
    private void checkStatus(int status, String msg) throws SDBPException {
        if (status < 0) {
            if (msg != null)
                throw new SDBPException(status, msg);
            if (status == Status.SDBP_BADSCHEMA)
                throw new SDBPException(status, "No database or table is in use");
            if (status == Status.SDBP_NOSERVICE)
                throw new SDBPException(status, "No server in the cluster was able to serve the request");
            throw new SDBPException(status, msg);
        }
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