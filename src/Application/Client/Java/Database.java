package com.scalien.scaliendb;

import java.math.BigInteger;
import java.util.List;
import java.util.ArrayList;

public class Database
{
    private Client client;
    private long databaseID;
    private String name;
    
    Database(Client client, long databaseID, String name) {
        this.client = client;
        this.databaseID = databaseID;
        this.name = name;
    }
    
    long getDatabaseID() {
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
        BigInteger biDatabaseID = BigInteger.valueOf(databaseID);
        long numTables = scaliendb_client.SDBP_GetNumTables(client.getPtr(), biDatabaseID);
        ArrayList<Table> tables = new ArrayList<Table>();
        for (long i = 0; i < numTables; i++) {
            long tableID = scaliendb_client.SDBP_GetTableIDAt(client.getPtr(), biDatabaseID, i).longValue();
            String name = scaliendb_client.SDBP_GetTableNameAt(client.getPtr(), biDatabaseID, i);
            tables.add(new Table(client, this, tableID, name));
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
        List<Table> tables = getTables();
        for (Table table : tables)
        {
            if (table.getName().equals(name))
                return table;
        }

        throw new SDBPException(Status.SDBP_BADSCHEMA);    
    }
    
    /**
     * Create a table in this database, with the first shard placed in the first available quorum.
     *
     * @param   name        the name of the table
     * @return              the table object
     */
    public Table createTable(String name) throws SDBPException {
        List<Quorum> quorums = client.getQuorums();
        if (quorums.size() == 0)
            throw new SDBPException(Status.SDBP_BADSCHEMA, "No quorums found");
        Quorum quorum = quorums.get(0);
        return createTable(name, quorum);
    }
    
    /**
     * Create a table in this database.
     *
     * @param   quorum      the quorum that will contain the table
     * @param   name        the name of the table
     * @return              the table object
     */
    public Table createTable(String name, Quorum quorum) throws SDBPException {
        BigInteger biDatabaseID = BigInteger.valueOf(databaseID);
        BigInteger biQuorumID = BigInteger.valueOf(quorum.getQuorumID());
        int status = scaliendb_client.SDBP_CreateTable(client.cptr, biDatabaseID, biQuorumID, name);
        client.checkResultStatus(status);
        return getTable(name);
    }    
    
    /**
     * Renames the database.
     *
     * @param   newName     the new name of the database
     */
    public void renameDatabase(String newName) throws SDBPException {
        BigInteger biDatabaseID = BigInteger.valueOf(databaseID);
        int status = scaliendb_client.SDBP_RenameDatabase(client.cptr, biDatabaseID, newName);
        client.checkResultStatus(status);
    }

    /**
     * Deletes the database.
     */
    public void deleteDatabase() throws SDBPException {
        BigInteger biDatabaseID = BigInteger.valueOf(databaseID);
        int status = scaliendb_client.SDBP_DeleteDatabase(client.cptr, biDatabaseID);
        client.checkResultStatus(status);
    }
}
