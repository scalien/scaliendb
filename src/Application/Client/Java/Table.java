package com.scalien.scaliendb;

import java.math.BigInteger;

public class Table
{
    private Client client;
    private Database database;
    private long tableID;
    private String name;
    
    public Table(Client client_, Database database_, String name_) throws SDBPException {
        client = client_;
        database = database_;
        name = name_;
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
 
 	public String get(String key) throws SDBPException {
        useDefaults();
        return client.get(key);
	}

 	public byte[] getData(byte[] key) throws SDBPException {
        useDefaults();
        return client.getData(key);
	}
		
	public int set(String key, String value) throws SDBPException {
        useDefaults();
        return client.set(key, value);
	}

	public int set(byte[] key, byte[] value) throws SDBPException {
        useDefaults();
        return client.set(key, value);
	}

	public int setIfNotExists(String key, String value) throws SDBPException {
        useDefaults();
        return client.setIfNotExists(key, value);
	}

	public int setIfNotExists(byte[] key, byte[] value) throws SDBPException {
        useDefaults();
        return client.setIfNotExists(key, value);
	}

	public int testAndSet(String key, String test, String value) throws SDBPException {
        useDefaults();
        return client.testAndSet(key, test, value);
	}

	public int testAndSet(byte[] key, byte[] test, byte[] value) throws SDBPException {
        useDefaults();
        return client.testAndSet(key, test, value);
	}

	public long add(String key, long number) throws SDBPException {
        useDefaults();
        return client.add(key, number);
	}

	public long add(byte[] key, long number) throws SDBPException {
        useDefaults();
        return client.add(key, number);
	}

	public int append(String key, String value) throws SDBPException {
        useDefaults();
        return client.append(key, value);
	}

	public int append(byte[] key, byte[] value) throws SDBPException {
        useDefaults();
        return client.append(key, value);
	}
	
	public int delete(String key) throws SDBPException {
        useDefaults();
        return client.delete(key);
	}

	public int delete(byte[] key) throws SDBPException {
        useDefaults();
        return client.delete(key);
	}

	public String remove(String key) throws SDBPException {
        useDefaults();
        return client.remove(key);
	}

	public String remove(byte[] key) throws SDBPException {
        useDefaults();
        return client.remove(key);
	}
}
