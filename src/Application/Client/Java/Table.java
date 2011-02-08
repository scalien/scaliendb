package com.scalien.scaliendb;

import java.math.BigInteger;

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
     * @see     Database#getTable(String name)  getTable
     */
    public Table(Client client, Database database, String name) throws SDBPException {
        this.client = client;
        this.database = database;
        this.name = name;
        BigInteger databaseID = BigInteger.valueOf(database.getDatabaseID());
        BigInteger bi = scaliendb_client.SDBP_GetTableID(client.getPtr(), databaseID, name);
        tableID = bi.longValue();
        if (tableID == 0)
            throw new SDBPException(Status.toString(Status.SDBP_BADSCHEMA));
    }

    private void useDefaults() throws SDBPException {
        client.useDatabase(database.getName());
        client.useTable(name);
    }

    /**
     * Renames the table.
     *
     * @param   newName     the new name of the table
     * @return              the status of the operation
     */
    public long renameTable(String newName) throws SDBPException {
        return client.renameTable(tableID, newName);
    }

    /**
     * Deletes the table.
     *
     * @return              the status of the operation
     */
    public long deleteTable() throws SDBPException {
        return client.deleteTable(tableID);
    }

    /**
     * Truncates the table.
     *
     * @return              the status of the operation
     */
    public long truncateTable() throws SDBPException {
        return client.truncateTable(tableID);
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
     * Associates the specified value with the specified key. If the database previously contained
     * a mapping for this key, the old value is replaced.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     * @return          the status of the operation
     */
	public int set(String key, String value) throws SDBPException {
        useDefaults();
        return client.set(key, value);
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
        useDefaults();
        return client.set(key, value);
	}

    /**
     * Associates the specified value with the specified key only if it did not exist previously.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     * @return          the status of the operation
     */
	public int setIfNotExists(String key, String value) throws SDBPException {
        useDefaults();
        return client.setIfNotExists(key, value);
	}

    /**
     * Associates the specified value with the specified key only if it did not exist previously.
     * 
     * @param   key     key with which the specified value is to be associated
     * @param   value   value to be associated with the specified key
     * @return          the status of the operation
     */
	public int setIfNotExists(byte[] key, byte[] value) throws SDBPException {
        useDefaults();
        return client.setIfNotExists(key, value);
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
     * @return          the status of the operation
     */
	public int testAndSet(byte[] key, byte[] test, byte[] value) throws SDBPException {
        useDefaults();
        return client.testAndSet(key, test, value);
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
     * @return          the status of the operation
     */
	public int append(String key, String value) throws SDBPException {
        useDefaults();
        return client.append(key, value);
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
        useDefaults();
        return client.append(key, value);
	}
	
    /**
     * Deletes the specified key.
     *
     * @param   key     key to be deleted
     * @return          the status of the operation
     */
	public int delete(String key) throws SDBPException {
        useDefaults();
        return client.delete(key);
	}

    /**
     * Deletes the specified key.
     *
     * @param   key     key to be deleted
     * @return          the status of the operation
     */
	public int delete(byte[] key) throws SDBPException {
        useDefaults();
        return client.delete(key);
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
}
