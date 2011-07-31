package com.scalien.scaliendb;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class Table
{
    private Client client;
    private Database database;
    private long tableID;
    private String name;
    
    /**
     * Creates a table object. Usually this is not used, instead request the table object from the 
     * database object with getTable.
     *
     * @param   client      the client object
     * @param   database    the database object
     * @param   name        the name of the table
     * @see     com.scalien.scaliendb.Database#getTable(String name)  getTable
     */
    public Table(Client client, Database database, String name) throws SDBPException {
        this.client = client;
        this.database = database;
        this.name = name;
        BigInteger databaseID = BigInteger.valueOf(database.getDatabaseID());
        BigInteger bi = scaliendb_client.SDBP_GetTableID(client.getPtr(), databaseID, name);
        tableID = bi.longValue();
        if (tableID == 0)
            throw new SDBPException(Status.SDBP_BADSCHEMA);
    }

    private void useDefaults() throws SDBPException {
        client.useDatabase(database.getDatabaseID());
        client.useTable(tableID);
    }

    /**
     * Renames the table.
     *
     * @param   newName     the new name of the table
     */
    public void renameTable(String newName) throws SDBPException {
        client.renameTable(tableID, newName);
    }

    /**
     * Deletes the table.
     */
    public void deleteTable() throws SDBPException {
        client.deleteTable(tableID);
    }

    /**
     * Truncates the table.
     */
    public void truncateTable() throws SDBPException {
        client.truncateTable(tableID);
    }
 
    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @return          the value if found
     */
    public String get(String key) throws SDBPException {
        useDefaults();
        return client.get(key);
    }

    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @return          the value if found
     */
    public byte[] get(byte[] key) throws SDBPException {
        useDefaults();
        return client.get(key);
    }

    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @param   defval  the default value
     * @return          the value if found, the default value if not found
     */
    public String get(String key, String defval) throws SDBPException {
        useDefaults();
        return client.get(key, defval);
    }

    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @param   defval  the default value
     * @return          the value if found, the default value if not found
     */
    public byte[] get(byte[] key, byte[] defval) throws SDBPException {
        useDefaults();
        return client.get(key, defval);
    }

    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @return          the value if found
     */
    public long getLong(String key) throws SDBPException {
        useDefaults();
        return Long.parseLong(client.get(key));
    }

    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @return          the value if found
     */
    public long getLong(byte[] key) throws SDBPException {
        useDefaults();
        return Long.parseLong(new String(client.get(key)));
    }

    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @return          the value if found
     */
    public long getLong(String key, long defval) throws SDBPException {
        useDefaults();
        return Long.parseLong(client.get(key, Long.toString(defval)));
    }

    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @return          the value if found
     */
    public long getLong(byte[] key, long defval) throws SDBPException {
        useDefaults();
        return Long.parseLong(new String(client.get(key, Long.toString(defval).getBytes())));
    }
    
    /**
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     */
    public void set(String key, String value) throws SDBPException {
        useDefaults();
        client.set(key, value);
    }

    /**
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     */
    public void set(byte[] key, byte[] value) throws SDBPException {
        useDefaults();
        client.set(key, value);
    }

    /**
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     */
    public void setLong(String key, long value) throws SDBPException {
        useDefaults();
        client.setLong(key, value);
    }

    /**
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     */
    public void setLong(byte[] key, long value) throws SDBPException {
        useDefaults();
        client.setLong(key, value);
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
        useDefaults();
        return client.add(key, number);
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
        useDefaults();
        return client.add(key, number);
    }
    
    /**
     * Deletes the specified key.
     *
     * @param   key     key to be deleted
     */
    public void delete(String key) throws SDBPException {
        useDefaults();
        client.delete(key);
    }

    /**
     * Deletes the specified key.
     *
     * @param   key     key to be deleted
     */
    public void delete(byte[] key) throws SDBPException {
        useDefaults();
        client.delete(key);
    }
    
    protected List<String> listKeys(String startKey, String endKey, String prefix, int count, boolean skip) throws SDBPException {
        useDefaults();
        return client.listKeys(startKey, endKey, prefix, count, skip);
    }

    protected List<byte[]> listKeys(byte[] startKey, byte[] endKey, byte[] prefix, int count, boolean skip) throws SDBPException {
        useDefaults();
        return client.listKeys(startKey, endKey, prefix, count, skip);
    }

    protected Map<String, String> listKeyValues(String startKey, String endKey, String prefix, int count, boolean skip) throws SDBPException {
        useDefaults();
        return client.listKeyValues(startKey, endKey, prefix, count, skip);
    }
    
    protected Map<byte[], byte[]> listKeyValues(byte[] startKey, byte[] endKey, byte[] prefix, int count, boolean skip) throws SDBPException {
        useDefaults();
        return client.listKeyValues(startKey, endKey, prefix, count, skip);
    }

    /**
     * Returns the number of key-value pairs.
     *
     * @param   ps          the count parameters, a StringRangeParams object
     *                      eg. (new StringRangeParams()).prefix("p").startKey("s").endKey("e")
     * @return              the number of key-value pairs
     */
    public long count(StringRangeParams ps) throws SDBPException {
        useDefaults();
        return client.count(ps);
    }
    
    /**
     * Returns the number of key-value pairs.
     *
     * @param   ps          the count params, a ByteRangeParams object 
     * @return              the number of key-value pairs
     */
    public long count(ByteRangeParams ps) throws SDBPException {
        useDefaults();
        return client.count(ps);
    }

    /**
     * Returns an Index object for the given key. Then use Index::Get() to retrieve new index values.
     *
     * @param   key         the index key
     * @return              the Index object
     */
    public Index getIndex(String key) throws SDBPException {
        return new Index(this.client, database.getDatabaseID(), tableID, key);
    }

    /**
     * Returns an Index object for the given key. Then use Index::Get() to retrieve new index values.
     *
     * @param   key         the index key
     * @return              the Index object
     */
    public Index getIndex(byte[] key) throws SDBPException {
        return new Index(this.client, database.getDatabaseID(), tableID, key);
    }

    /**
     * Returns a key iterator over keys.
     *
     * @param   ps          the iteration parameters, a StringRangeParams object
     *                      eg. (new StringRangeParams()).prefix("p").startKey("s").endKey("e")
     */
    public StringKeyIterator getKeyIterator(StringRangeParams ps) throws SDBPException {
        return new StringKeyIterator(this, ps);
    }

    /**
     * Returns a key iterator over keys.
     *
     * @param   ps          the iteration params, a ByteRangeParams object 
     */
    public ByteKeyIterator getKeyIterator(ByteRangeParams ps) throws SDBPException {
        return new ByteKeyIterator(this, ps);
    }

    /**
     * Returns a key-value iterator over keys.
     *
     * @param   ps          the iteration parameters, a StringRangeParams object
     *                      eg. (new StringRangeParams()).prefix("p").startKey("s").endKey("e")
     */
    public StringKeyValueIterator getKeyValueIterator(StringRangeParams ps) throws SDBPException {
        return new StringKeyValueIterator(this, ps);
    }

    /**
     * Returns a key-value iterator over keys.
     *
     * @param   ps          the iteration params, a ByteRangeParams object
     */
    public ByteKeyValueIterator getKeyValueIterator(ByteRangeParams ps) throws SDBPException {
        return new ByteKeyValueIterator(this, ps);
    }
}
