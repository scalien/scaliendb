package com.scalien.scaliendb;

/**
 * Query is a class for creating iterators.
 *
 * @see Table#createQuery() createQuery
 */
public class Query
{
    private Table table;
    private byte[] prefix;
    private byte[] startKey;
    private byte[] endKey;
    private int count;
    
    Query(Table table) {
        this.table = table;
        prefix = new byte[0];
        startKey = new byte[0];
        endKey = new byte[0];
        count = 0;
    }
    
    /**
     * Specify the prefix parameter for iteration.
     * <p>Only keys starting with prefix will be returned by the iteration.
     * @param prefix The prefix parameter as a String.
     */    
    public void setPrefix(String prefix) {
        this.prefix = prefix.getBytes();
    }
    
    /**
     * Specify the prefix parameter for iteration.
     * <p>Only keys starting with prefix will be returned by the iteration.
     * @param prefix The prefix parameter as a String.
     */    
    public void setPrefix(byte[] prefix) {
        this.prefix = prefix;
    }
    
    /**
     * Specify the start key parameter for iteration.
     * <p>Iteration will start at start key, or the first key greater than start key.
     * @param startKey The start key parameter as a String.
     */
    public void setStartKey(String startKey) {
        this.startKey = startKey.getBytes();
    }
    
    /**
     * Specify the start key parameter for iteration.
     * <p>Iteration will start at start key, or the first key greater than start key.
     * @param startKey The start key parameter as a byte[].
     */
    public void setStartKey(byte[] startKey) {
        this.startKey = startKey;
    }
    
    /**
     * Specify the end key parameter for iteration
     * <p>Iteration will stop at end key, or the first key greater than end key.
     * @param endKey The end key parameter as a String.
     */
    public void setEndKey(String endKey) {
        this.endKey = endKey.getBytes();
    }
    
    /**
     * Specify the end key parameter for iteration
     * <p>Iteration will stop at end key, or the first key greater than end key.
     * @param endKey The end key parameter as a byte[].
     */
    public void setEndKey(byte[] endKey) {
        this.endKey = endKey;
    }
    
    /**
     * Specify the count parameter for iteration
     * <p>Iteration will stop after count elements.
     * @param count The count parameter.
     */    
    public void setCount(int count) {
        this.count = count;
    }
    
    /**
     * Return an iterator that will return only keys.
     * @return The iterator.
     * @see #getStringKeyIterator() 
     * @see #getByteKeyValueIterator() 
     * @see #getStringKeyValueIterator() 
     */
    public ByteKeyIterator getByteKeyIterator() throws SDBPException {
        ByteRangeParams params = new ByteRangeParams();

        params.prefix(prefix);
        params.startKey(startKey);
        params.endKey(endKey);
        params.count(count);

        return new ByteKeyIterator(table, params);
    }
    
    /**
     * Return an iterator that will return keys and values as a <code>KeyValue&lt;byte[], byte[]&gt;</code>.
     * @return The iterator.
     * @see #getByteKeyIterator() 
     * @see #getStringKeyIterator() 
     * @see #getStringKeyValueIterator() 
     */
    public ByteKeyValueIterator getByteKeyValueIterator() throws SDBPException {
        ByteRangeParams params = new ByteRangeParams();

        params.prefix(prefix);
        params.startKey(startKey);
        params.endKey(endKey);
        params.count(count);
        
        return new ByteKeyValueIterator(table, params);
    }
    
    /**
     * Return an iterator that will return only keys.
     * <p>
     * Example:
     * <pre>
     * db = client.getDatabase("testDatabase");
     * table = db.getTable("testTable");
     * query = table.createQuery()
     * query.setPrefix("foo");
     * for (String s : query.getKeyIterator())
     *     System.out.println(s);
     * </pre>
     * @return The iterator.
     * @see #getByteKeyIterator() 
     * @see #getByteKeyValueIterator() 
     * @see #getStringKeyValueIterator() 
     */
    public StringKeyIterator getStringKeyIterator() throws SDBPException {
        StringRangeParams params = new StringRangeParams();

        params.prefix(new String(prefix));
        params.startKey(new String(startKey));
        params.endKey(new String(endKey));
        params.count(count);

        return new StringKeyIterator(table, params);
    }

    /**
     * Return an iterator that will return keys and values as a <code>KeyValue&lt;String, String&gt;</code>.
     * <p>
     * Example:
     * <pre>
     * db = client.getDatabase("testDatabase");
     * table = db.getTable("testTable");
     * query = table.createQuery()
     * query.setPrefix("foo");
     * for (KeyValue&lt;String, String&gt; kv in table.getKeyValueIterator())
     *     System.out.println(kv.getKey() + " => " + kv.getValue());
     * </pre>
     * @return The iterator.
     * @see #getByteKeyIterator() 
     * @see #getStringKeyIterator() 
     * @see #getByteKeyValueIterator() 
     */
    public StringKeyValueIterator getStringKeyValueIterator() throws SDBPException {
        StringRangeParams params = new StringRangeParams();

        params.prefix(new String(prefix));
        params.startKey(new String(startKey));
        params.endKey(new String(endKey));
        params.count(count);

        return new StringKeyValueIterator(table, params);
    }
}