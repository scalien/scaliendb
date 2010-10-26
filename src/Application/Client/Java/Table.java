package com.scalien.scaliendb;

public class Table
{
    private Client client;
    private long databaseID;
    private long tableID;
    private String name;
    
    public Table(Client client_, long databaseID_, String name_) {
        client = client_;
        databaseID = databaseID_;
        name = name_;
        tableID = scaliendb_client.SDBP_GetTableID(client.getPtr(), databaseID, name);
        if (tableID == 0)
            throw new SDBPException(Status.toString(Status.SDBP_BADSCHEMA));
    }
 
 	public String get(String key) throws SDBPException {
        return client.get(databaseID, tableID, key);
	}
		
	public int set(String key, String value) throws SDBPException {
        return client.set(databaseID, tableID, key, value);
	}
	
	public int delete(String key) throws SDBPException {
        return client.delete(databaseID, tableID, key);
	}
      
}
