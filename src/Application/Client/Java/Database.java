package com.scalien.scaliendb;

import java.math.BigInteger;
import java.util.List;
import java.util.ArrayList;

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
     * @see     com.scalien.scaliendb.Client#getDatabase(String name)
     */
    public Database(Client client, String name) throws SDBPException {
        this.client = client;
        this.name = name;
        BigInteger bi = scaliendb_client.SDBP_GetDatabaseID(client.getPtr(), name);
        databaseID = bi.longValue();
        if (databaseID == 0)
            throw new SDBPException(Status.SDBP_BADSCHEMA);
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
     * Returns all tables.
     *
     * @return              a list of table objects
     */
    public List<Table> getTables() throws SDBPException {
        long numTables = scaliendb_client.SDBP_GetNumTables(client.getPtr());
        ArrayList<Table> tables = new ArrayList<Table>();
        for (long i = 0; i < numTables; i++) {
            String name = scaliendb_client.SDBP_GetTableNameAt(client.getPtr(), i);
            tables.add(new Table(client, this, name));
        }
        return tables;
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
    
    /**
     * Creates a table.
     *
     * @param   quorum      the quorum that will contain the table
     * @param   name        the name of the table
     * @return              the table object
     */
    public Table createTable(Quorum quorum, String name) throws SDBPException {
        long createdTableID = client.createTable(databaseID, quorum.getQuorumID(), name);
        long numTables = scaliendb_client.SDBP_GetNumTables(client.getPtr());
        for (long i = 0; i < numTables; i++) {
            BigInteger bi = scaliendb_client.SDBP_GetTableIDAt(client.getPtr(), i);
            long tableID = bi.longValue();
            if (tableID == createdTableID)
                return new Table(client, this, name);
        }
        throw new SDBPException(Status.SDBP_API_ERROR, "Cannot find created table!");
    }    
    
    /**
     * Renames the database.
     *
     * @param   newName     the new name of the database
     */
    public void renameDatabase(String newName) throws SDBPException {
        client.renameDatabase(databaseID, newName);
    }

    /**
     * Deletes the database.
     */
    public void deleteDatabase() throws SDBPException {
        client.deleteDatabase(databaseID);
    }
}
