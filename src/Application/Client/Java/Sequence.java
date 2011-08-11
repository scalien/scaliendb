package com.scalien.scaliendb;

public class Sequence
{
    private Client client;
    private long tableID;
    private String stringKey;
    private byte[] byteKey;

    private long gran = 1000;
    private long seq = 0;
    private long num = 0;

    Sequence(Client client, long tableID, String key) {
        this.client = client;
        this.tableID = tableID;
        this.stringKey = key;
    }

    Sequence(Client client, long tableID, byte[] key) {
        this.client = client;
        this.tableID = tableID;
        this.byteKey = key;
    }
    
    /**
     * Set the granularity of the seqeunce increments (default 1000).
     *
     * @param   ps  The granularity of the sequence.
     */
    public void setGranularity(long gran)
    {
        this.gran = gran;
    }

    /**
     * Reset the sequence to 0. The next time <see cref="GetNext"/> is called, 0 will be returned.
     */
    public void reset()  throws SDBPException {
            if (stringKey != null)
                client.set(tableID, stringKey, "0");
            else
                client.set(tableID, byteKey, "0".getBytes());
            num = 0;
    }

    /**
     * Get the next sequence value.
     *
     * @return  The next sequence value.
     */
    public long get()  throws SDBPException {
        if (num == 0)
            AllocateRange();
        
        num--;
        return seq++;
    }

    private void AllocateRange() throws SDBPException {
        if (stringKey != null)
            seq = client.add(tableID, stringKey, gran) - gran;
        else
            seq = client.add(tableID, byteKey, gran) - gran;
        num = gran;
    }
}
