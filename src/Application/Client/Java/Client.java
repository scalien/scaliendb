package com.scalien.scaliendb;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;

/**
 * The ScalienDB client represents a connection to a ScalienDB cluster.
 * <p>
 * Example:
 * <pre>
 * Client client = new Client("localhost:7080");
 * db = client.getDatabase("testDatabase");
 * table = db.getTable("testTable");
 * // some sets
 * for (i = 0; i < 1000; i++) 
 *    table.set("foo" + i, "foo" + i);
 * for (i = 0; i < 1000; i++) 
 *    table.set("bar" + i, "bar" + i);
 * // some deletes
 * table.delete("foo0");
 * table.delete("foo10");
 * client.submit();
 * // count
 * System.out.println("number of keys starting with foo: " + table.count(new StringRangeParams().prefix("foo")));
 * // iterate
 * for (KeyValue&lt;String, String&gt; kv : table.getKeyValueIterator(new StringRangeParams().prefix("bar")))
 *     System.out.println(kv.getKey() + " => " + kv.getValue());
 * // truncate
 * table.truncate();
 * // don't forge to close() at the end, it sends off any batched commands!
 * client.close();
 * </pre>
 */
public class Client
{
    static {
        System.loadLibrary("scaliendb_client");
    }

    SWIGTYPE_p_void cptr;
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
     * Turns on or off the debug trace functionality.
     */
    public static void setTrace(boolean trace) {
        scaliendb_client.SDBP_SetTrace(trace);
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
     * Sets the consistency mode for read operations.
     *
     * <p>There are separate consistency modes the client can operate at.
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
     * @param   consistencyMode    can be CONSISTENCY_ANY, CONSISTENCY_RYW or CONSISTENCY_STRICT
     */
    public void setConsistencyMode(int consistencyMode) {
        scaliendb_client.SDBP_SetConsistencyMode(cptr, consistencyMode);
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
    public Quorum getQuorum(String name) throws SDBPException {
        List<Quorum> quorums = getQuorums();
        for (Quorum quorum : quorums)
        {
            if (quorum.getName().equals(name))
                return quorum;
        }
        throw new SDBPException(Status.SDBP_BADSCHEMA, "Quorum not found");
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
            String quorumName = scaliendb_client.SDBP_GetQuorumNameAt(cptr, i);
            quorums.add(new Quorum(this, quorumID, quorumName));
        }
        return quorums;
    }
    
    /**
     * Returns the specified database.
     *
     * @param   name    the name of the database
     * @return          the specified database
     */    
    public Database getDatabase(String name) throws SDBPException {
        List<Database> databases = getDatabases();
        for (Database database : databases)
        {
            if (database.getName().equals(name))
                return database;
        }
        throw new SDBPException(Status.SDBP_BADSCHEMA);
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
            BigInteger bi = scaliendb_client.SDBP_GetDatabaseIDAt(cptr, i);
            long databaseID = bi.longValue();            
            String name = scaliendb_client.SDBP_GetDatabaseNameAt(cptr, i);
            databases.add(new Database(this, databaseID, name));
        }
        return databases;
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
        return getDatabase(name);
    }
    
    String get(long tableID, String key) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_Get(cptr, biTableID, key);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            checkStatus(status);
        }
                        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        return result.getValue();
    }

    byte[] get(long tableID, byte[] key) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_GetCStr(cptr, biTableID, key, key.length);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            checkStatus(status);
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        return result.getValueBytes();
    }
    
    String get(long tableID, String key, String defval) {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_Get(cptr, biTableID, key);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return defval;
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        return result.getValue();
    }

    byte[] get(long tableID, byte[] key, byte[] defval) {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_GetCStr(cptr, biTableID, key, key.length);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            return defval;
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        return result.getValueBytes();
    }

    void set(long tableID, String key, String value) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_Set(cptr, biTableID, key, value);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            if (status == Status.SDBP_API_ERROR)
                checkStatus(status, "Batch limit exceeded");
            else
                checkStatus(status);
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
    }

    void set(long tableID, byte[] key, byte[] value) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_SetCStr(cptr, biTableID, key, key.length, value, value.length);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            if (status == Status.SDBP_API_ERROR)
                checkStatus(status, "Batch limit exceeded");
            else
                checkStatus(status);
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
    }

    long add(long tableID, String key, long number) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_Add(cptr, biTableID, key, number);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            checkStatus(status);
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        return result.getSignedNumber();
    }

    long add(long tableID, byte[] key, long number) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_AddCStr(cptr, biTableID, key, key.length, number);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            checkStatus(status);
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        return result.getSignedNumber();
    }
        
    void delete(long tableID, String key) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_Delete(cptr, biTableID, key);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            if (status == Status.SDBP_API_ERROR)
                checkStatus(status, "Batch limit exceeded");
            else
                checkStatus(status);
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
    }

    void delete(long tableID, byte[] key) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_DeleteCStr(cptr, biTableID, key, key.length);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            if (status == Status.SDBP_API_ERROR)
                checkStatus(status, "Batch limit exceeded");
            else
                checkStatus(status);
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
    }

    long count(long tableID, StringRangeParams ps) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_Count(cptr, biTableID, ps.startKey, ps.endKey, ps.prefix);
        checkResultStatus(status);        
        return result.getNumber();
    }
    
    long count(long tableID, ByteRangeParams ps) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_CountCStr(cptr, biTableID, ps.startKey, ps.startKey.length, ps.endKey, ps.endKey.length, ps.prefix, ps.prefix.length);
        checkResultStatus(status);
        return result.getNumber();
    }
    
    protected List<String> listKeys(long tableID, String startKey, String endKey, String prefix, int count, boolean skip)
    throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_ListKeys(cptr, biTableID, startKey, endKey, prefix, count, skip);
        checkResultStatus(status);

        ArrayList<String> keys = new ArrayList<String>();
        for (result.begin(); !result.isEnd(); result.next())
            keys.add(result.getKey());

        return keys;
    }

    protected List<byte[]> listKeys(long tableID, byte[] startKey, byte[] endKey, byte[] prefix, int count, boolean skip)
    throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_ListKeysCStr(cptr, biTableID, startKey, startKey.length, 
        endKey, endKey.length, prefix, prefix.length, count, skip);
        checkResultStatus(status);

        ArrayList<byte[]> keys = new ArrayList<byte[]>();
        for (result.begin(); !result.isEnd(); result.next())
            keys.add(result.getKeyBytes());

        return keys;
    }

    protected Map<String, String> listKeyValues(long tableID, String startKey, String endKey, String prefix, int count, boolean skip)
    throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_ListKeyValues(cptr, biTableID, startKey, endKey, prefix, count, skip);
        checkResultStatus(status);
            
        TreeMap<String, String> keyValues = new TreeMap<String, String>();
        for (result.begin(); !result.isEnd(); result.next())
            keyValues.put(result.getKey(), result.getValue());
        
        return keyValues;
    }

    protected Map<byte[], byte[]> listKeyValues(long tableID, byte[] startKey, byte[] endKey, byte[] prefix, int count, boolean skip) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_ListKeyValuesCStr(cptr, biTableID, startKey, startKey.length, endKey, endKey.length, prefix, prefix.length, count, skip);
        checkResultStatus(status);
            
        TreeMap<byte[], byte[]> keyValues = new TreeMap<byte[], byte[]>(new ByteArrayComparator());
        for (result.begin(); !result.isEnd(); result.next())
            keyValues.put(result.getKeyBytes(), result.getValueBytes());
        return keyValues;
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
    public void rollback() throws SDBPException {
        int status = scaliendb_client.SDBP_Cancel(cptr);
        checkStatus(status);        
    }

    void checkResultStatus(int status) throws SDBPException {
        checkResultStatus(status, null);
    }

    void checkResultStatus(int status, String msg) throws SDBPException {
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        checkStatus(status, msg);
    }
    
    void checkStatus(int status) throws SDBPException {
        checkStatus(status, null);
    }

    void checkStatus(int status, String msg) throws SDBPException {
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
}