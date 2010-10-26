package com.scalien.scaliendb;

import java.math.BigInteger;

public class Database
{
    private Client client;
    private long databaseID;
    private String name;
    
    public Database(Client client_, String name_) throws SDBPException {
        client = client_;
        BigInteger bi = scaliendb_client.SDBP_GetDatabaseID(client.getPtr(), name);
        databaseID = bi.longValue();
        if (databaseID == 0)
            throw new SDBPException(Status.toString(Status.SDBP_BADSCHEMA));
        name = name_;
    }
    
    public long getDatabaseID() {
        return databaseID;
    }
    
    public String getName() {
        return name;
    }
    
    public Table getTable(String name) throws SDBPException {
        return new Table(client, this, name);
    }
}