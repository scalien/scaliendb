package com.scalien.scaliendb;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class Table
{
    Client client;
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
    Table(Client client, Database database, long tableID, String name) throws SDBPException {
        this.client = client;
        this.database = database;
        this.tableID = tableID;
        this.name = name;
    }
    
    Client getClient()
    {
        return client;
    }
    
    long getTableID()
    {
        return tableID;
    }
    
    public String getName()
    {
        return name;
    }

    /**
     * Renames the table.
     *
     * @param   newName     the new name of the table
     */
    public void renameTable(String newName) throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_RenameTable(client.cptr, biTableID, newName);
        client.checkResultStatus(status);    
    }

    /**
     * Deletes the table.
     */
    public void deleteTable() throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_DeleteTable(client.cptr, biTableID);
        client.checkResultStatus(status);    
    }

    /**
     * Truncates the table.
     */
    public void truncateTable() throws SDBPException {
        BigInteger biTableID = BigInteger.valueOf(tableID);
        int status = scaliendb_client.SDBP_TruncateTable(client.cptr, biTableID);
        client.checkResultStatus(status);    
    }
 
    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @return          the value if found
     */
    public String get(String key) throws SDBPException {
        return client.get(tableID, key);
    }

    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @return          the value if found
     */
    public byte[] get(byte[] key) throws SDBPException {
        return client.get(tableID, key);
    }

    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @param   defval  the default value
     * @return          the value if found, the default value if not found
     */
    public String get(String key, String defval) throws SDBPException {
        return client.get(tableID, key, defval);
    }

    /**
     * Returns the value for a specified key.
     *
     * @param   key     the specified key
     * @param   defval  the default value
     * @return          the value if found, the default value if not found
     */
    public byte[] get(byte[] key, byte[] defval) throws SDBPException {
        return client.get(tableID, key, defval);
    }
    
    /**
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     */
    public void set(String key, String value) throws SDBPException {
        client.set(tableID, key, value);
    }

    /**
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     */
    public void set(byte[] key, byte[] value) throws SDBPException {
        client.set(tableID, key, value);
    }
    
    /**
     * Deletes the specified key.
     *
     * @param   key     key to be deleted
     */
    public void delete(String key) throws SDBPException {
        client.delete(tableID, key);
    }

    /**
     * Deletes the specified key.
     *
     * @param   key     key to be deleted
     */
    public void delete(byte[] key) throws SDBPException {
        client.delete(tableID, key);
    }
    
    /**
     * Returns the number of key-value pairs.
     *
     * @param   ps          the count parameters, a StringRangeParams object
     *                      eg. (new StringRangeParams()).prefix("p").startKey("s").endKey("e")
     * @return              the number of key-value pairs
     */
    public long count(StringRangeParams ps) throws SDBPException {
        return client.count(tableID, ps);
    }
    
    /**
     * Returns the number of key-value pairs.
     *
     * @param   ps          the count params, a ByteRangeParams object 
     * @return              the number of key-value pairs
     */
    public long count(ByteRangeParams ps) throws SDBPException {
        return client.count(tableID, ps);
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

    /**
     * Returns a Sequence object for the given key. Then use Sequence::Get() to retrieve new sequence values.
     *
     * @param   key         the index key
     * @return              the Index object
     */
    public Sequence getSequence(String key) throws SDBPException {
        return new Sequence(this.client, tableID, key);
    }

    /**
     * Returns an Sequence object for the given key. Then use Sequence::Get() to retrieve new sequence values.
     *
     * @param   key         the index key
     * @return              the Index object
     */
    public Sequence getSequence(byte[] key) throws SDBPException {
        return new Sequence(this.client, tableID, key);
    }
}
