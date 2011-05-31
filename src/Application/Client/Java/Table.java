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
    public <K, V> void set(K key, V value) throws SDBPException {
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
     * Associates the specified value with the specified key only if it did not exist previously.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     * @return          true if the value was set
     */
    public boolean setIfNotExists(String key, String value) throws SDBPException {
        useDefaults();
        return client.setIfNotExists(key, value);
    }

    /**
     * Associates the specified value with the specified key only if it did not exist previously.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     * @return          true if the value was set
     */
    public boolean setIfNotExists(byte[] key, byte[] value) throws SDBPException {
        useDefaults();
        return client.setIfNotExists(key, value);
    }

    /**
     * Associates the specified value with the specified key only if it did not exist previously.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     * @return          true if the value was set
     */
    public <K, V> boolean setIfNotExists(K key, V value) throws SDBPException {
        useDefaults();
        return client.setIfNotExists(key, value);        
    }

    /**
     * Associates the specified value with the specified key only if it did not exist previously.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     * @return          true if the value was set
     */
    public boolean setIfNotExistsLong(String key, long value) throws SDBPException {
        useDefaults();
        return client.setIfNotExistsLong(key, value);
    }

    /**
     * Associates the specified value with the specified key only if it did not exist previously.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     * @return          true if the value was set
     */
    public boolean setIfNotExistsLong(byte[] key, long value) throws SDBPException {
        useDefaults();
        return client.setIfNotExistsLong(key, value);
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
     * @return          true if the value was set
     */
    public boolean testAndSet(String key, String test, String value) throws SDBPException {
        useDefaults();
        return client.testAndSet(key, test, value);
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
     * @return          true if the value was set
     */
    public boolean testAndSet(byte[] key, byte[] test, byte[] value) throws SDBPException {
        useDefaults();
        return client.testAndSet(key, test, value);
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
     * @return          true if the value was set
     */
    public <K, V> boolean testAndSet(K key, V test, V value) throws SDBPException {
        useDefaults();
        return client.testAndSet(key, test, value);
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
     * @return          true if the value was set
     */
    public boolean testAndSetLong(String key, long test, long value) throws SDBPException {
        useDefaults();
        return client.testAndSetLong(key, test, value);
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
     * @return          true if the value was set
     */
    public boolean testAndSetLong(byte[] key, long test, long value) throws SDBPException {
        useDefaults();
        return client.testAndSetLong(key, test, value);
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
        useDefaults();
        return client.getAndSet(key, value);
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
        useDefaults();
        return client.getAndSet(key, value);
    }

    /**
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced and returned.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     * @return          the old value
     */
    public long getAndSetLong(String key, long value) throws SDBPException {
        useDefaults();
        return client.getAndSetLong(key, value);
    }
    
    /**
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced and returned.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     * @return          the old value
     */
    public long getAndSetLong(byte[] key, long value) throws SDBPException {
        useDefaults();
        return client.getAndSetLong(key, value);
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
     * Appends the specified value to end of the value of the specified key. If the key did not
     * exist, it is created with the specified value.
     *
     * @param   key     key to which the specified value is to be appended
     * @param   value   the specified value that is appended to end of the existing value
     */
    public void append(String key, String value) throws SDBPException {
        useDefaults();
        client.append(key, value);
    }

    /**
     * Appends the specified value to end of the value of the specified key. If the key did not
     * exist, it is created with the specified value.
     *
     * @param   key     key to which the specified value is to be appended
     * @param   value   the specified value that is appended to end of the existing value
     */
    public void append(byte[] key, byte[] value) throws SDBPException {
        useDefaults();
        client.append(key, value);
    }

    /**
     * Appends the specified value to end of the value of the specified key. If the key did not
     * exist, it is created with the specified value.
     *
     * @param   key     key to which the specified value is to be appended
     * @param   value   the specified value that is appended to end of the existing value
     */
    public <K, V> void append(K key, V value) throws SDBPException {
        useDefaults();
        client.append(key, value);
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

    /**
     * Deletes the specified key.
     *
     * @param   key     key to be deleted
     */
    public <K> void delete(K key) throws SDBPException {
        useDefaults();
        client.delete(key);
    }
    
    /**
     * Deletes the specified key only if it matches a specified test value.
     * 
     * The testAndDelete command conditionally and atomically deletes a key => value pair, but only 
     * if the current value matches the user specified test value.
     *
     * @param   key     key with which the specified value is to be associated
     * @param   test    the user specified value that is tested against the old value
     * @return          true if the key was deleted
     */
    public boolean testAndDelete(String key, String test) throws SDBPException {
        useDefaults();
        return client.testAndDelete(key, test);
    }

    /**
     * Deletes the specified key only if it matches a specified test value.
     * 
     * The testAndDelete command conditionally and atomically deletes a key => value pair, but only 
     * if the current value matches the user specified test value.
     *
     * @param   key     key with which the specified value is to be associated
     * @param   test    the user specified value that is tested against the old value
     * @return          true if the key was deleted
     */
    public boolean testAndDelete(byte[] key, byte[] test) throws SDBPException {
        useDefaults();
        return client.testAndDelete(key, test);
    }

    /**
     * Deletes the specified key only if it matches a specified test value.
     * 
     * The testAndDelete command conditionally and atomically deletes a key => value pair, but only 
     * if the current value matches the user specified test value.
     *
     * @param   key     key with which the specified value is to be associated
     * @param   test    the user specified value that is tested against the old value
     * @return          true if the key was deleted
     */
    public <K, V> boolean testAndDelete(K key, V test) throws SDBPException {
        useDefaults();
        return client.testAndDelete(key, test);
    }

    /**
     * Deletes the specified key only if it matches a specified test value.
     * 
     * The testAndDelete command conditionally and atomically deletes a key => value pair, but only 
     * if the current value matches the user specified test value.
     *
     * @param   key     key with which the specified value is to be associated
     * @param   test    the user specified value that is tested against the old value
     * @return          true if the key was deleted
     */
    public boolean testAndDeleteLong(String key, long test) throws SDBPException {
        useDefaults();
        return client.testAndDeleteLong(key, test);
    }

    /**
     * Deletes the specified key only if it matches a specified test value.
     * 
     * The testAndDelete command conditionally and atomically deletes a key => value pair, but only 
     * if the current value matches the user specified test value.
     *
     * @param   key     key with which the specified value is to be associated
     * @param   test    the user specified value that is tested against the old value
     * @return          true if the key was deleted
     */
    public boolean testAndDelete(byte[] key, long test) throws SDBPException {
        useDefaults();
        return client.testAndDeleteLong(key, test);
    }

    /**
     * Deletes the specified key and returns the old value.
     *
     * @param   key     key to be deleted
     * @return          the old value
     */
    public String remove(String key) throws SDBPException {
        useDefaults();
        return client.remove(key);
    }

    /**
     * Deletes the specified key and returns the old value.
     *
     * @param   key     key to be deleted
     * @return          the old value
     */
    public byte[] remove(byte[] key) throws SDBPException {
        useDefaults();
        return client.remove(key);
    }
    
    /**
     * Returns the specified keys.
     *
     * @param   startKey    listing starts at this key
     * @param   endKey      listing ends at this key
     * @param   prefix      list only those keys that starts with prefix
     * @param   offset      specifies the offset of the first key to return
     * @param   count       specifies the number of keys returned
     * @return              the list of keys
     */
    public List<String> listKeys(String startKey, String endKey, String prefix, int offset, int count) throws SDBPException {
        useDefaults();
        return client.listKeys(startKey, endKey, prefix, offset, count);
    }

    /**
     * Returns the specified keys.
     *
     * @param   startKey    listing starts at this key
     * @param   endKey      listing ends at this key
     * @param   prefix      list only those keys that starts with prefix
     * @param   offset      specifies the offset of the first key to return
     * @param   count       specifies the number of keys returned
     * @return              the list of keys
     */
    public List<byte[]> listKeys(byte[] startKey, byte[] endKey, byte[] prefix, int offset, int count) throws SDBPException {
        useDefaults();
        return client.listKeys(startKey, endKey, prefix, offset, count);
    }

    /**
     * Returns the specified key-value pairs.
     *
     * @param   startKey    listing starts at this key
     * @param   endKey      listing ends at this key
     * @param   prefix      list only those keys that starts with prefix
     * @param   offset      specifies the offset of the first key to return
     * @param   count       specifies the number of keys returned
     * @return              the list of key-value pairs
     */
    public Map<String, String> listKeyValues(String startKey, String endKey, String prefix, int offset, int count) throws SDBPException {
        useDefaults();
        return client.listKeyValues(startKey, endKey, prefix, offset, count);
    }
    
    /**
     * Returns the specified key-value pairs.
     *
     * @param   startKey    listing starts at this key
     * @param   endKey      listing ends at this key
     * @param   prefix      list only those keys that starts with prefix
     * @param   offset      specifies the offset of the first key to return
     * @param   count       specifies the number of keys returned
     * @return              the list of key-value pairs
     */
    public Map<byte[], byte[]> listKeyValues(byte[] startKey, byte[] endKey, byte[] prefix, int offset, int count) throws SDBPException {
        useDefaults();
        return client.listKeyValues(startKey, endKey, prefix, offset, count);
    }

    /**
     * Returns the number of key-value pairs.
     *
     * @param   startKey    counting starts at this key
     * @param   endKey      counting ends at this key
     * @param   prefix      count only those keys that starts with prefix
     * @param   offset      specifies the offset of the first key to return
     * @param   count       specifies the maximum number of keys returned
     * @return              the number of key-value pairs
     */
    public long count(String startKey, String endKey, String prefix, int offset, int count) throws SDBPException {
        useDefaults();
        return client.count(startKey, endKey, prefix, offset, count);
    }
    
    /**
     * Returns the number of key-value pairs.
     *
     * @param   startKey    counting starts at this key
     * @param   endKey      counting ends at this key
     * @param   prefix      count only those keys that starts with prefix
     * @param   offset      specifies the offset of the first key to return
     * @param   count       specifies the maximum number of keys returned
     * @return              the number of key-value pairs
     */
    public long count(byte[] startKey, byte[] endKey, byte[] prefix, int offset, int count) throws SDBPException {
        useDefaults();
        return client.count(startKey, endKey, prefix, offset, count);    
    }
}
