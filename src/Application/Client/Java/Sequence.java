package com.scalien.scaliendb;

/**
 * Sequence is a class for atomically retrieving unique integer IDs
 * using a key whose value stores the next ID as a piece of text.
 * The first returned ID is 1.
 * <p>
 * ScalienDB is a key value store, so unique IDs have to be maintained
 * in a separate key in a seperate table. For example:
 * <p>
 * <code>people => 2500</code>
 * <p>
 * In this case, the next sequence value returned would be 2500.
 * <p>
 * ScalienDB has a special <code>ADD</code> command which parses the value as a number
 * and increments it by a user specified value.
 * The Index class wraps this functionality, and increments the number by a
 * user-defined <code>gran</code> (default 1000). This way, the server is contacted
 * only every 1000th time the sequence is incremented, which is an important
 * optimization since sending and waiting for commands to execute on the server
 * is slow.
 * <p>
 * Note that if the client quits before using up all the sequence values retrieved
 * by the last <code>ADD</code> command, those will not be passed out, and there will
 * be a hole in the sequence. 
 * <p>
 * Use <code>Reset()</code> to reset the sequence to 1. This sets:
 * <p>
 * <code>
 * key => 1
 * </code>
 * <p>
 * Example:
 * <pre>
 * db = client.getDatabase("testDatabase");
 * indices = db.getTable("indices");
 * Sequence peopleIDs = indices.getSequence("people");
 * for (var i = 0; i < 1000; i++)
 *     System.out.println(peopleIDs.Get());
 * </pre>
 * @see Table#getSequence(String) getSequence
 * @see Table#getSequence(byte[]) getSequence
 */
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
     * Set the gran of the seqeunce increments (default 1000).
     * @param gran The gran of the sequence.
     */
    public void setGranularity(long gran)
    {
        this.gran = gran;
    }

    /**
     * Reset the sequence to 1. The next time getNext() is called, 1 will be returned.
     * <p>
     * This sets:
     * <p>
     * <pre>
     * key => 0
     * </pre>
     */
     public void reset()  throws SDBPException {
            if (stringKey != null)
                client.delete(tableID, stringKey);
            else
                client.delete(tableID, byteKey);
            num = 0;
    }

    /**
     * Get the next sequence value.
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
