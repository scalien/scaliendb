package com.scalien.scaliendb;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/**
 * Table is a convenience class for encapsulating table related operations.
 * <p>
 * ScalienDB uses databases and tables to manage key value namespaces.
 * <p>
 * Example:
 * <pre>
 * db = client.getDatabase("testDatabase");
 * table = db.getTable("testTable");
 * // some sets
 * for (i = 0; i < 1000; i++) 
 *     table.set("foo" + i, "foo" + i);
 * client.submit();
 * for (i = 0; i < 1000; i++) 
 *     table.set("bar" + i, "bar" + i);
 * client.submit()
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
 * </pre>
 * @see Database#createTable(String) 
 * @see Database#createTable(String, Quorum)
 * @see Database#getTable(String) 
 * @see Database
 */
 public class Table
{
    Client client;
    private Database database;
    private long tableID;
    private String name;
    
    Table(Client client, Database database, long tableID, String name) throws SDBPException {
        this.client = client;
        this.database = database;
        this.tableID = tableID;
        this.name = name;
    }
    
    Client getClient() {
        return client;
    }
    
    long getTableID() {
        return tableID;
    }
    
    /** 
     * The database this table is in.
     */
    public Database getDatabase() {
        return database;
    }
    
    /**
     * The name of the table.
     */
    public String getName() {
        return name;
    }

    /**
     * Create a new instance of a query.
     */
    public Query createQuery() {
        return new Query(this);
    }

    /**
     * Rename the table.
     * @param newName The new name of the table.
     * @exception SDBPException 
     */
    public void renameTable(String newName) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_RenameTable(client.cptr, biTableID, newName);
        client.checkResultStatus(status);    
    }

    /**
     * Delete the table. This means all key-values in the table are dropped.
     * @exception SDBPException 
     */
    public void deleteTable() throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_DeleteTable(client.cptr, biTableID);
        client.checkResultStatus(status);    
    }

    /**
     * Truncate the table.
     * @exception SDBPException 
     */
    public void truncateTable() throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_TruncateTable(client.cptr, biTableID);
        client.checkResultStatus(status);    
    }
 
    /**
     * Retrieve a value by key from the table. Returns <code>null</code> if not found.
     * @param key The key to look for.
     * @return The retrieved value.
     * @exception SDBPException 
     * @see #get(byte[]) 
     */
    public String get(String key) throws SDBPException {
        return client.get(tableID, key);
    }

    /**
     * Retrieve a value by key from the table. Returns <code>defval</code> if not found.
     * @param key The key to look for.
     * @param defval The default return value.
     * @return The retrieved value.
     * @exception SDBPException 
     * @see #get(byte[], byte[]) 
     */
    public String get(String key, String defval) throws SDBPException {
        String value = client.get(tableID, key);
        if (value != null)
            return value;
        else
            return defval;
    }

    /**
     * Retrieve a value by key from the table. Returns <code>null</code> if not found.
     * @param key The key to look for.
     * @return The retrieved value.
     * @exception SDBPException 
     * @see #get(String) 
     */
    public byte[] get(byte[] key) throws SDBPException {
        return client.get(tableID, key);
    }

    /**
     * Retrieve a value by key from the table. Returns <code>defval</code> if not found.
     * @param key The key to look for.
     * @param defval The default return value.
     * @return The retrieved value.
     * @exception SDBPException 
     * @see #get(String, String) 
     */
    public byte[] get(byte[] key, byte[] defval) throws SDBPException {
        byte[] value = client.get(tableID, key);
        if (value != null)
            return value;
        else
            return defval;
    }
    
    /**
     * Set a key-value in the table.
     * @param key The key.
     * @param value The value.
     * @exception SDBPException 
     * @see #set(byte[], byte[]) 
     */
    public void set(String key, String value) throws SDBPException {
        client.set(tableID, key, value);
    }

    /**
     * Set a key-value in the table.
     * @param key The key.
     * @param value The value.
     * @exception SDBPException 
     * @see #set(String, String) 
     */
    public void set(byte[] key, byte[] value) throws SDBPException {
        client.set(tableID, key, value);
    }
    
    /**
     * Delete a key-value pair by key in the table.
     * @param key The key.
     * @exception SDBPException 
     * @see #delete(byte[]) 
     */
    public void delete(String key) throws SDBPException {
        client.delete(tableID, key);
    }

    /**
     * Delete a key-value pair by key in the table.
     * @param key The key.
     * @exception SDBPException 
     * @see #delete(String) 
     */
    public void delete(byte[] key) throws SDBPException {
        client.delete(tableID, key);
    }
    
    /**
     * Return the number of matching keys in the table.
     * @param ps The filter parameters.
     * @return The number of matching keys in the table.
     * @exception SDBPException 
     * @see StringRangeParams 
     * @see #count(ByteRangeParams) 
     */
    public long count(StringRangeParams ps) throws SDBPException {
        return client.count(tableID, ps);
    }
    
    /**
     * Return the number of matching keys in the table.
     * @param ps The filter parameters.
     * @return The number of matching keys in the table.
     * @exception SDBPException 
     * @see ByteRangeParams 
     * @see #count(StringRangeParams) 
     */
    public long count(ByteRangeParams ps) throws SDBPException {
        return client.count(tableID, ps);
    }

    /**
     * Return an iterator that will return only keys.
     * @param ps The parameters of iteration, as a <a href="StringRangeParams.html">StringRangeParams</a>.
     * <p>
     * Example:
     * <pre>
     * db = client.getDatabase("testDatabase");
     * table = db.getTable("testTable");
     * for (String s : table.getKeyIterator(new StringRangeParams().prefix("foo")))
     *     System.out.println(s);
     * </pre>
     * @return The iterator.
     * @see StringRangeParams 
     * @see #getKeyIterator(ByteRangeParams) 
     * @see #getKeyValueIterator(StringRangeParams) 
     * @see #getKeyValueIterator(ByteRangeParams) 
     */
    public StringKeyIterator getKeyIterator(StringRangeParams ps) throws SDBPException {
        return new StringKeyIterator(this, ps);
    }

    /**
     * Return an iterator that will return only keys.
     * @param ps The parameters of iteration, as a <a href="ByteRangeParams.html">ByteRangeParams</a>.
     * @return The iterator.
     * @see ByteRangeParams 
     * @see #getKeyIterator(StringRangeParams) 
     * @see #getKeyValueIterator(StringRangeParams) 
     * @see #getKeyValueIterator(ByteRangeParams) 
     */
    public ByteKeyIterator getKeyIterator(ByteRangeParams ps) throws SDBPException {
        return new ByteKeyIterator(this, ps);
    }

    /**
     * Return an iterator that will return keys and values as a <code>KeyValue&lt;String, String&gt;</code>.
     * @param ps The parameters of iteration, as a <a href="StringRangeParams.html">StringRangeParams</a>.
     * <p>
     * Example:
     * <pre>
     * db = client.getDatabase("testDatabase");
     * table = db.getTable("testTable");
     * for (KeyValue&lt;String, String&gt; kv in table.getKeyValueIterator(new StringRangeParams().prefix("foo")))
     *     System.out.println(kv.getKey() + " => " + kv.getValue());
     * </pre>
     * @return The iterator.
     * @see StringRangeParams 
     * @see #getKeyValueIterator(ByteRangeParams) 
     * @see #getKeyIterator(StringRangeParams) 
     * @see #getKeyIterator(ByteRangeParams) 
     */
    public StringKeyValueIterator getKeyValueIterator(StringRangeParams ps) throws SDBPException {
        return new StringKeyValueIterator(this, ps);
    }

    /**
     * Return an iterator that will return keys and values as a <code>KeyValue&lt;byte[], byte[]&gt;</code>.
     * @param ps The parameters of iteration, as a <a href="ByteRangeParams.html">ByteRangeParams</a>.
     * @return The iterator.
     * @see ByteRangeParams 
     * @see #getKeyValueIterator(StringRangeParams) 
     * @see #getKeyIterator(StringRangeParams) 
     * @see #getKeyIterator(ByteRangeParams) 
     */
    public ByteKeyValueIterator getKeyValueIterator(ByteRangeParams ps) throws SDBPException {
        return new ByteKeyValueIterator(this, ps);
    }
}
