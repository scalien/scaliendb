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
 * @see Database#getSequenceNumber(String, String) getSequenceNumber
 * @see Database#resetSequenceNumber(String, String) resetSequenceNumber
 * @see Database#setSequenceGranularity(String, String, long) setSequenceGranularity
 */
public class Sequence
{
    private byte[] name;
    private long tableID;

    Sequence(long tableID, String name) {
        this.tableID = tableID;
        this.name = name.getBytes();
    }

    Sequence(long tableID, byte[] name) {
        this.tableID = tableID;
        this.name = name;
    }
    
    /**
     * @deprecated
     * Set the gran of the seqeunce increments (default 1000).
     * @param gran The gran of the sequence.
     */
    public synchronized void setGranularity(long gran) {
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
    public synchronized void reset(Client client) throws SDBPException {
        client.sequenceSet(tableID, name, 1);
    }

    /**
     * Reset the sequence to value. The next time getNext() is called, value will be returned.
     * <p>
     * This sets:
     * <p>
     * <pre>
     * key => value
     * </pre>
     */
    public synchronized void set(Client client, long value) throws SDBPException {
        client.sequenceSet(tableID, name, value);
    }

    /**
     * Get the next sequence value.
     */
    public synchronized long get(Client client) throws SDBPException {
        return client.sequenceNext(tableID, name);
    }
}
