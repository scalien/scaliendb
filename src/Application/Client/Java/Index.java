package com.scalien.scaliendb;

public class Index
{
    private Client client;
    private long databaseID;
    private long tableID;
    private String stringKey;
    private byte[] byteKey;

    private long count = 1000;
    private long index = 0;
    private long num = 0;

    public Index(Client client, long databaseID, long tableID, String key) {
        this.client = client;
        this.databaseID = databaseID;
        this.tableID = tableID;
        this.stringKey = key;
    }

    public Index(Client client, long databaseID, long tableID, byte[] key) {
        this.client = client;
        this.databaseID = databaseID;
        this.tableID = tableID;
        this.byteKey = key;
    }
    
    /**
     * Sets the granularity for this index proxy.
     *
     * @param   ps          granularity, default 1000
     */
    public void setGranularity(long count)
    {
        this.count = count;
    }

    /**
     * Retrieves the next unique index value.
     *
     * @return              the next unique index value
     */
    public long get()  throws SDBPException {
        if (num == 0)
            AllocateIndexRange();
        
        num--;
        return index++;
    }

    /**
     * Resets the index to 0 (on the server).
     * WARNING: use with care, the index will no longer be unique!
     *
     */
    public void reset()  throws SDBPException {
        long oldDatabaseID = client.getDatabaseID();
        long oldTableID = client.getTableID();

        if (oldDatabaseID != databaseID)
            client.useDatabase(databaseID);
        if (oldTableID != tableID)
            client.useTable(tableID);

        if (stringKey != null)
            client.set(stringKey, "0");
        else
            client.set(byteKey, "0".getBytes());
        num = 0;

        if (oldDatabaseID != databaseID)
            client.useDatabase(oldDatabaseID);
        if (oldTableID != tableID)
            client.useTable(oldTableID);
    }

    private void AllocateIndexRange() throws SDBPException {
        long oldDatabaseID = client.getDatabaseID();
        long oldTableID = client.getTableID();

        if (oldDatabaseID != databaseID)
            client.useDatabase(databaseID);
        if (oldTableID != tableID)
            client.useTable(tableID);

        if (stringKey != null)
            index = client.add(stringKey, count) - count;
        else
            index = client.add(byteKey, count) - count;
        num = count;

        if (oldDatabaseID != databaseID)
            client.useDatabase(oldDatabaseID);
        if (oldTableID != tableID)
            client.useTable(oldTableID);
    }
}
