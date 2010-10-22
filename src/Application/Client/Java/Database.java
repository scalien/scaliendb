package com.scalien.scaliendb;

public class Database
{
    private Client client;
    private long databaseID;
    private String name;
    
    public Database(Client client_, String name) {
        client = client_;
        databaseID = scaliendb_client.SDBP_GetDatabaseID(client.getPtr(), name);
        if (databaseID == 0)
            throw new SDBPException(Status.toString(Status.SDBP_BADSCHEMA));            
    }
    
    public long getDatabaseID() {
        return databaseID;
    }
    
    public String getName() {
        return name;
    }
    
    public Table getTable(String name) {
        return new Table(client, databaseID, name);
    }
}