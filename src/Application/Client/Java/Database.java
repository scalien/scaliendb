package com.scalien.scaliendb;

import java.math.BigInteger;

public class Database
{
    private Client client;
    private long databaseID;
    private String name;
    
    /**
     * Creates a database object. Usually this is not used, instead request the database object
     * from the client object with getDatabase.
     *
     * @param   client  the client object
     * @param   name    the name of the database
     * @see     Client#getDatabase(String name)  getDatabase
     */
    public Database(Client client, String name) throws SDBPException {
        this.client = client;
        this.name = name;
        BigInteger bi = scaliendb_client.SDBP_GetDatabaseID(client.getPtr(), name);
        databaseID = bi.longValue();
        if (databaseID == 0)
            throw new SDBPException(Status.toString(Status.SDBP_BADSCHEMA));
    }
    
    /**
     * Returns the ID of the database.
     */
    public long getDatabaseID() {
        return databaseID;
    }
    
    /**
     * Returns the name of the database.
     */
    public String getName() {
        return name;
    }
    
    /**
     * Returns a table object with the specified name from the database. If the table is not found
     * an exception is thrown.
     *
     * @param   name    the name of the table
     * @return          the table object
     */
    public Table getTable(String name) throws SDBPException {
        return new Table(client, this, name);
    }
}
