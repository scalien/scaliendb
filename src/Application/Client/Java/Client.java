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

    /**
     * Get operations are served by any quorum member. May return stale data.
     * @see #setConsistencyMode(int) 
     */
    public static final int CONSISTENCY_ANY     = 0;
    
    /**
     * Get operations are served using read-your-writes semantics.
     * @see #setConsistencyMode(int) 
     */
    public static final int CONSISTENCY_RYW     = 1;

    /**
     * Get operations are always served from the primary of each quorum.
     * This is the default consistency level.
     * @see #setConsistencyMode(int) 
     */
    public static final int CONSISTENCY_STRICT  = 2;

    /**
     * Default batch mode. Writes are batched, then sent off to the ScalienDB server.
     * @see #setBatchMode(int) 
     * @see #setBatchLimit(long) 
     */
    public static final int BATCH_DEFAULT       = 0;

    /**
     * Writes are batched, and if the batch limit is reached an exception is thrown.
     * @see #setBatchMode(int) 
     * @see #setBatchLimit(long) 
     */
    public static final int BATCH_NOAUTOSUBMIT  = 1;

    /**
     * Writes are sent one by one. This will be much slower than the default mode.
     * @see #setBatchMode(int) 
     * @see #setBatchLimit(long) 
     */
    public static final int BATCH_SINGLE        = 2;
    
    /**
     * Construct a Client object. Pass in the list of controllers
     * as ellipsed strings in the form "host:port".
     * <p>
     * Example:
     * <pre>
     * Client client = new Client("192.168.1.1:7080", "192.168.1.2:7080", "192.168.1.3:7080");
     * </pre>
     * @param nodes The controllers as ellipsed list of strings in the form "host:port".
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
     * <p>
     * You must call close() at the end of the program to clean up after the client instance.
     * Also, close() automatically calls <a href="#submit()">submit()</a> to send off any batched commands.
     * @see #submit()
     * @see #rollback()
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
    
    /**
     * Turn on additional log trace output in the underlying C++ client library. Off by default.
     * @param trace true or false.
     */
    public static void setTrace(boolean trace) {
        scaliendb_client.SDBP_SetTrace(trace);
    }
    
    /**
     * Sets the name of the trace log output file in the underlying C++ client library.
     * @param filename the name of the logfile.
     */
    public static void setLogFile(String filename) {
        scaliendb_client.SDBP_SetLogFile(filename);
    }

   /**
     * Send a log message thru the unedrlying C++ client library.
     * @param msg the message.
     */
    public static void logTrace(String msg) {
        scaliendb_client.SDBP_LogTrace(msg);
    }


    /**
     * The maximum time the client library will wait to complete operations, in miliseconds.
     * Default 120 seconds.
     * @param timeout The global timeout in miliseconds.
     * @see #setMasterTimeout(long) 
     * @see #getGlobalTimeout() 
     * @see #getMasterTimeout() 
     */
    public void setGlobalTimeout(long timeout) {
        scaliendb_client.SDBP_SetGlobalTimeout(cptr, BigInteger.valueOf(timeout));
    }
    
    /**
     * The maximum time the client library will wait to find the master node, in miliseconds.
     * Default 21 seconds.
     * @param timeout The master timeout in miliseconds. 
     * @see #setGlobalTimeout(long) 
     * @see #getGlobalTimeout() 
     * @see #getMasterTimeout() 
     */
    public void setMasterTimeout(long timeout) {
        scaliendb_client.SDBP_SetMasterTimeout(cptr, BigInteger.valueOf(timeout));
    }
    
    /**
     * Get the global timeout in miliseconds. The global timeout is the maximum time
     * the client library will wait to complete operations.
     * @return The global timeout in miliseconds.
     * @see #getMasterTimeout() 
     * @see #setGlobalTimeout(long) 
     * @see #setMasterTimeout(long) 
     */
    public long getGlobalTimeout() {
        BigInteger bi = scaliendb_client.SDBP_GetGlobalTimeout(cptr);
        return bi.longValue();
    }
    
    /**
     * Get the master timeout in miliseconds. The master timeout is the maximum time
     * the client library will wait to find the master node.
     * @return The master timeout in miliseconds.
     * @see #getGlobalTimeout() 
     * @see #setGlobalTimeout(long) 
     * @see #setMasterTimeout(long) 
     */
    public long getMasterTimeout() {
        BigInteger bi = scaliendb_client.SDBP_GetMasterTimeout(cptr);
        return bi.longValue();
    }
    
    /**
     * Set the consistency mode for Get operations.
     * <p>
     * Possible values are:
     * <ul>
     * <li><a href="#CONSISTENCY_STRICT">CONSISTENCY_STRICT</a>: Get operations are sent to the primary of the quorum.</li>
     * <li><a href="#CONSISTENCY_RYW">CONSISTENCY_RYW</a>: RYW is short for Read Your Writers. Get operations may be
     * sent to slaves, but the internal replication number is used as a sequencer to make sure
     * the slave has seen the last write issued by the client.</li>
     * <li><a href="#CONSISTENCY_ANY">CONSISTENCY_ANY</a>: Get operations may be sent to slaves, which may return stale data.</li>
     * </ul>
     * <p>
     * The default is <a href="#CONSISTENCY_STRICT">CONSISTENCY_STRICT</a>.
     * @param consistencyMode <a href="#CONSISTENCY_STRICT">CONSISTENCY_STRICT</a> or
     * <a href="#CONSISTENCY_RYW">CONSISTENCY_RYW</a> or
     * <a href="#CONSISTENCY_ANY">CONSISTENCY_ANY</a>
     */
    public void setConsistencyMode(int consistencyMode) {
        scaliendb_client.SDBP_SetConsistencyMode(cptr, consistencyMode);
    }

    /**
     * Set the batch mode for write operations (Set and Delete).
     * <p>
     * As a performance optimization, the ScalienDB can collect write operations in client memory and send them
     * off in batches to the servers. This will drastically improve the performance of the database.
     * To send off writes excplicitly, use <a href="#submit()">submit()</a>.
     * <p>
     * The modes are:
     * <ul>
     * <li><a href="#BATCH_DEFAULT">BATCH_DEFAULT</a>:
     *     collect writes until the batch limit is reached, then send them off to the servers</li>
     * <li><a href="#BATCH_NOAUTOSUBMIT">BATCH_NOAUTOSUBMIT</a>:
     *     collect writes until the batch limit is reached, then throw an <a href="SDBPException.html">SDBPException</a></li>
     * <li><a href="#BATCH_SINGLE">BATCH_SINGLE</a>:
    *      send writes one by one, as they are issued</li>
     * </ul>
     * <p>
     * The default is <a href="#BATCH_DEFAULT">BATCH_DEFAULT</a>.
     * @param batchMode <a href="#">BATCH_DEFAULT</a> or
     * <a href="#BATCH_NOAUTOSUBMIT">BATCH_NOAUTOSUBMIT</a> or
     * <a href="#BATCH_SINGLE">BATCH_SINGLE</a>
     * @see #setBatchLimit(long) 
     */
    public void setBatchMode(int batchMode) {
        scaliendb_client.SDBP_SetBatchMode(cptr, batchMode);
    }
    
    /**
     * The the batch limit for write operations (Set and Delete) in bytes.
     * <p>
     * When running with batchMode = <a href="#BATCH_DEFAULT">BATCH_DEFAULT</a> and
     * <a href="#BATCH_NOAUTOSUBMIT">BATCH_NOAUTOSUBMIT</a>, the client collects writes
     * and sends them off to the ScalienDB server in batches.
     * To send off writes excplicitly, use <a href="#submit()">submit()</a>.
     * <p>setBatchLimit() lets you specify the exact amount to store in memory before:
     * <ul>
     * <li>submiting the operations in <a href="#BATCH_DEFAULT">BATCH_DEFAULT</a> mode</li>
     * <li>throwing an <a href="SDBPException.html">SDBPException</a> in
     * <a href="#BATCH_NOAUTOSUBMIT">BATCH_NOAUTOSUBMIT</a> mode</li>
     * </ul>
     * <p>
     * The default 1MB.
     * @param batchLimit The batch limit in bytes.
     * @see #setBatchMode(int) 
     */
    public void setBatchLimit(long batchLimit) {
        scaliendb_client.SDBP_SetBatchLimit(cptr, batchLimit);
    }

    /**
     * Returns the config state in a JSON-serialized string.
     */
    public String getJSONConfigState() {
        return scaliendb_client.SDBP_GetJSONConfigState(cptr);
    }

    /**
     * Return a <a href="Quorum.html">Quorum</a> by name.
     * @param name The quorum name.
     * @return The <a href="Quorum.html">Quorum</a> object or <code>null</code>.
     * @see Quorum
     * @see #getQuorums()
     */
    public Quorum getQuorum(String name) throws SDBPException {
        BigInteger bi = scaliendb_client.SDBP_GetQuorumIDByName(cptr, name);
        long quorumID = bi.longValue();
        if (quorumID == 0)
            throw new SDBPException(Status.SDBP_BADSCHEMA, "Database not found: " + name);
        return new Quorum(this, quorumID, name);
    }

    /**
     * Return all quorums in the database as a list of <a href="Quorum.html">Quorum</a> objects.
     * @return The list of <a href="Quorum.html">Quorum</a> objects.
     * @see Quorum 
     * @see #getQuorum(String) 
     */
    public List<Quorum> getQuorums() throws SDBPException {
        long numQuorums = scaliendb_client.SDBP_GetNumQuorums(cptr);
        ArrayList<Quorum> quorums = new ArrayList<Quorum>();
        for (long i = 0; i < numQuorums; i++) {
            BigInteger bi = scaliendb_client.SDBP_GetQuorumIDAt(cptr, i);
            long quorumID = bi.longValue();
            String name = scaliendb_client.SDBP_GetQuorumNameAt(cptr, i);
            quorums.add(new Quorum(this, quorumID, name));
        }
        return quorums;
    }
    
    /**
     * Get a <a href="Database.html">Database</a> by name.
     * @param name The name of the database.
     * @return The <a href="Database.html">Database</a> object or <code>null</code>.
     * @see Database
     * @see #getDatabases()
     * @see #createDatabase(String)
     */
    public Database getDatabase(String name) throws SDBPException {
        BigInteger bi = scaliendb_client.SDBP_GetDatabaseIDByName(cptr, name);
        long databaseID = bi.longValue();
        if (databaseID == 0)
            throw new SDBPException(Status.SDBP_BADSCHEMA, "Database not found: " + name);
        return new Database(this, databaseID, name);
    }

    /**
     * Get all databases as a list of <a href="Database.html">Database</a> objects.
     * @return A list of <a href="Database.html">Database</a> objects.
     * @see Database 
     * @see #getDatabase(String) 
     * @see #createDatabase(String) 
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
     * Create a database and return the corresponding <a href="Database.html">Database</a> object.
     * @param name The name of the database to create.
     * @return The new <a href="Database.html">Database</a> object.
     * @see Database 
     * @see #getDatabase(String) 
     * @see #getDatabases() 
     */    
    public Database createDatabase(String name) throws SDBPException {
        int status = scaliendb_client.SDBP_CreateDatabase(cptr, name);
        checkResultStatus(status);
        return getDatabase(name);
    }
    
    String get(long tableID, String key) throws SDBPException {
        return byteArrayToString(get(tableID, stringToByteArray(key));
    }

    byte[] get(long tableID, byte[] key) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_GetCStr(cptr, biTableID, key, key.length);
        if (status < 0) {
            if (status == Status.SDBP_FAILED)
                return null;
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            checkStatus(status);
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        return result.getValueBytes();
    }
    
    void set(long tableID, String key, String value) throws SDBPException {
        set(tableID, stringToByteArray(key), stringToByteArray(value));
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
        return add(tableID, stringToByteArray(key), number);
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
        delete(tableID, stringToByteArray(key));
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

    void sequenceSet(long tableID, String key, long number) throws SDBPException {
        sequenceset(tableID, stringToByteArray(key), number);
    }

    void sequenceSet(long tableID, byte[] key, long number) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        BigInteger biNumber = BigInteger.valueOf(number);
        int status = scaliendb_client.SDBP_SequenceSetCStr(cptr, biTableID, key, key.length, biNumber);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            checkStatus(status);
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
    }

    long sequenceNext(long tableID, String key) throws SDBPException {
        return sequenceNext(tableID, stringToByteArray(key));
    }

    long sequenceNext(long tableID, byte[] key) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_SequenceNextCStr(cptr, biTableID, key, key.length);
        if (status < 0) {
            result = new Result(scaliendb_client.SDBP_GetResult(cptr));
            checkStatus(status);
        }
        
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        return result.getNumber();
    }

    long count(long tableID, StringRangeParams ps) throws SDBPException {
        return count(tableID, ps.toByteRangeParams());
    }
    
    long count(long tableID, ByteRangeParams ps) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_CountCStr(
         cptr, biTableID, ps.startKey, ps.startKey.length, ps.endKey, ps.endKey.length,
         ps.prefix, ps.prefix.length, ps.forwardDirection);
        checkResultStatus(status);
        return result.getNumber();
    }
    
    protected List<String> listKeys(long tableID, String startKey, String endKey, String prefix, int count, boolean forwardDirection, boolean skip)
    throws SDBPException {
        List<byte[]> byteKeys = listKeys(tableID, stringToByteArray(startKey), stringToByteArray(endKey), stringToByteArray(prefix), count, forwardDirection, skip);
        ArrayList<String> stringKeys = new ArrayList<String>()
        for (byte[] key in byteKeys)
            stringKeys.add(byteArrayToString(key));
        return stringKeys;
    }

    protected List<byte[]> listKeys(long tableID, byte[] startKey, byte[] endKey, byte[] prefix, int count, boolean forwardDirection, boolean skip)
    throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_ListKeysCStr(
         cptr, biTableID, startKey, startKey.length, 
         endKey, endKey.length, prefix, prefix.length, count, forwardDirection, skip);
         checkResultStatus(status);

        ArrayList<byte[]> keys = new ArrayList<byte[]>();
        for (result.begin(); !result.isEnd(); result.next())
            keys.add(result.getKeyBytes());

        return keys;
    }

    protected Map<String, String> listKeyValues(long tableID, String startKey, String endKey, String prefix, int count, boolean forwardDirection, boolean skip)
    throws SDBPException {
        Map<byte[], byte[]> byteKeyValues = listKeyValues(tableID, stringToByteArray(startKey), stringToByteArray(endKey), stringToByteArray(prefix), count, forwardDirection, skip);
        TreeMap<String, String> stringKeyValues = new TreeMap<String, String>();
        for (KeyValue<byte[], byte[]> kv : byteKeyValues)
            stringKeyValues.put(byteArrayToString(kv.getKey()), byteArrayToString(kv.getValue()));
        return stringKeyValues;
    }

    protected Map<byte[], byte[]> listKeyValues(long tableID, byte[] startKey, byte[] endKey, byte[] prefix, int count, boolean forwardDirection, boolean skip) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_ListKeyValuesCStr(cptr, biTableID,
         startKey, startKey.length, endKey, endKey.length, prefix, prefix.length,
         count, forwardDirection, skip);
        checkResultStatus(status);
            
        TreeMap<byte[], byte[]> keyValues = new TreeMap<byte[], byte[]>(new ByteArrayComparator());
        for (result.begin(); !result.isEnd(); result.next())
            keyValues.put(result.getKeyBytes(), result.getValueBytes());
        return keyValues;
    }
    
    /**
     * Send the batched operations to the ScalienDB server.
     * @see #rollback()
     */
    public void submit() throws SDBPException {
        int status = scaliendb_client.SDBP_Submit(cptr);
        result = new Result(scaliendb_client.SDBP_GetResult(cptr));
        checkStatus(status);
    }
    
    /**
     * Discard all batched operations.
     * @see #submit()
     */
    public void rollback() throws SDBPException {
        int status = scaliendb_client.SDBP_Cancel(cptr);
        checkStatus(status);        
    }

    static byte[] stringToByteArray(String s) {
        return s.getBytes("UTF8")
    }

    static String byteArrayToString(byte[] b) {
        return new String(b, "UTF8");
    }
}