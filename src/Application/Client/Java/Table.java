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
		
	public int set(String key, String value) throws SDBPException {
        useDefaults();
        return client.set(key, value);
	}

	public int setIfNotExists(String key, String value) throws SDBPException {
        useDefaults();
        return client.setIfNotExists(key, value);
	}

	public int testAndSet(String key, String test, String value) throws SDBPException {
        useDefaults();
        return client.testAndSet(key, test, value);
	}

	public long add(String key, long number) throws SDBPException {
        useDefaults();
        return client.add(key, number);
	}
	
	public int delete(String key) throws SDBPException {
        useDefaults();
        return client.delete(key);
	}

	public String remove(String key) throws SDBPException {
        useDefaults();
        return client.remove(key);
	}
}
