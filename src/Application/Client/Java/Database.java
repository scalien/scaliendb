package com.scalien.scaliendb;

import java.math.BigInteger;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Database is a convenience class for encapsulating database related operations.
 * <p>
 * ScalienDB uses databases and tables to manage key value namespaces.
 * <p>
 * Example:
 * <pre>
 * db = client.getDatabase("testDatabase");
 * table = db.getTable("testTable");
 * table.set("foo", "bar");
 * </pre>
 * @see Client#createDatabase(String)
 * @see Client#getDatabase(String)
 * @see Table
 * @see Quorum
 */
public class Database
{
    private Client client;
    private long databaseID;
    private String name;
    private static HashMap<String, Sequence> sequences = new HashMap<String, Sequence>();
    
    Database(Client client, long databaseID, String name) {
        this.client = client;
        this.databaseID = databaseID;
        this.name = name;
    }
    
    long getDatabaseID() {
        return databaseID;
    }
    
    /**
     * Return the name of the database.
     */
    public String getName() {
        return name;
    }

    /**
     * Retrieve a <a href="Table.html">Table</a> in this database by name.
     * @param name The name of the table.
     * @return The corresponding <a href="Table.html">Scalien.Table</a> object or <code>null</code>.
     * @exception SDBPException 
     * @see Table 
     */
    public Table getTable(String name) throws SDBPException {
        BigInteger biDatabaseID = BigInteger.valueOf(databaseID);
        BigInteger bi = scaliendb_client.SDBP_GetTableIDByName(client.getPtr(), biDatabaseID, name);
        long tableID = bi.longValue();
        if (tableID == 0)
            throw new SDBPException(Status.SDBP_BADSCHEMA, "Table not found: " + name);
        return new Table(client, this, tableID, name);
    }

    /**
     * Retrieve the tables in the database as a list of <a href="Table.html">Table</a> objects.
     * @return The list of table objects.
     * @exception SDBPException 
     * @see Table 
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
     * Create a table in this database, with the first shard placed in the first available quorum.
     * @param name The name of the table.
     * @return The <a href="Table.html">Table</a> object corresponding to the created table.
     * @exception SDBPException 
     * @see Table 
     * @see #createTable(String, Quorum) 
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
     * @param quorum The first shard of the table is placed inside the <a href="Quorum.html">Quorum</a>.
     * @param name The name of the table.
     * @return The <a href="Table.html">Table</a> object corresponding to the created table.
     * @exception SDBPException 
     * @see #createTable(String) 
     */
    public Table createTable(String name, Quorum quorum) throws SDBPException {
        BigInteger biDatabaseID = BigInteger.valueOf(databaseID);
        BigInteger biQuorumID = BigInteger.valueOf(quorum.getQuorumID());
        int status = scaliendb_client.SDBP_CreateTable(client.cptr, biDatabaseID, biQuorumID, name);
        client.checkResultStatus(status);
        return getTable(name);
    }    
    
    /**
     * Rename the database.
     * @param newName The new database name.
     * @exception SDBPException 
     */
    public void renameDatabase(String newName) throws SDBPException {
        BigInteger biDatabaseID = BigInteger.valueOf(databaseID);
        int status = scaliendb_client.SDBP_RenameDatabase(client.cptr, biDatabaseID, newName);
        client.checkResultStatus(status);
    }

    /**
     * Delete the database.
     * @exception SDBPException 
     */
    public void deleteDatabase() throws SDBPException {
        BigInteger biDatabaseID = BigInteger.valueOf(databaseID);
        int status = scaliendb_client.SDBP_DeleteDatabase(client.cptr, biDatabaseID);
        client.checkResultStatus(status);
    }
    
    /**
     * Returns the next sequence number for a given name
     * @param tableName The name of the sequence table
     * @param sequenceName The name of the sequence
     * @exception SDBPException
     */
    public long getSequenceNumber(String tableName, String sequenceName) throws SDBPException {
        String separator = "/";
        String sequenceID = databaseID + separator + tableName + separator + sequenceName;

        Sequence sequence;
        synchronized (sequences) {
            sequence = sequences.get(sequenceID);
            if (sequence == null)
            {
                Table sequenceTable = getTable(tableName);
                sequence = new Sequence(sequenceTable.getTableID(), sequenceName);
                sequences.put(sequenceID, sequence);
            }
        }
        return sequence.get(client);        
    }

    /**
     * Resets the sequence number to 1.
     * @param tableName The name of the sequence table
     * @param sequenceName The name of the sequence
     * @exception SDBPException
     */
    public void resetSequenceNumber(String tableName, String sequenceName) throws SDBPException {
        String separator = "/";
        String sequenceID = databaseID + separator + tableName + separator + sequenceName;

        Sequence sequence;
        synchronized (sequences) {
            sequence = sequences.get(sequenceID);
            if (sequence == null)
            {
                Table sequenceTable = getTable(tableName);
                sequence = new Sequence(sequenceTable.getTableID(), sequenceName);
                sequences.put(sequenceID, sequence);
            }
        }
        sequence.reset(client);
    }

    /**
     * Resets the sequence number to value.
     * @param tableName The name of the sequence table
     * @param sequenceName The name of the sequence
     * @exception SDBPException
     */
    public void setSequenceNumber(String tableName, String sequenceName, long value) throws SDBPException {
        String separator = "/";
        String sequenceID = databaseID + separator + tableName + separator + sequenceName;

        Sequence sequence;
        synchronized (sequences) {
            sequence = sequences.get(sequenceID);
            if (sequence == null)
            {
                Table sequenceTable = getTable(tableName);
                sequence = new Sequence(sequenceTable.getTableID(), sequenceName);
                sequences.put(sequenceID, sequence);
            }
        }
        sequence.set(client, value);
    }

    /**
     * @deprecated
     * Sets the sequence granularity.
     * @param tableName The name of the sequence table
     * @param sequenceName The name of the sequence
     * @exception SDBPException
     */
    public void setSequenceGranularity(String tableName, String sequenceName, long granularity) throws SDBPException {
    }
}
