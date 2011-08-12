package com.scalien.scaliendb;

import java.math.BigInteger;

/**
 * Quorum is a convenience class for working wit quorums.
 * <p>
 * Example:
 * <pre>
 * Quorum quorum = client.getQuorum("quorum 1");
 * // create table located in quorum
 * Database db = client.getDatabase("testDatabase");
 * db.createTable("testTable", quorum)
 * // list all quorums
 * for (Quorum q : client.getQuorums())
 *     System.out.println(q.Name);
 * </pre>
 * @see Client#getQuorum(String) 
 * @see Client#getQuorums() 
 * @see Database#createTable(String, Quorum) 
 */
public class Quorum
{
    private Client client;
    private long quorumID;
    private String name;
    
    Quorum(Client client, long quorumID, String name) throws SDBPException {
        this.client = client;
        this.quorumID = quorumID;
        this.name = name;
    }
    
    long getQuorumID() {
        return quorumID;
    }

    /**
     * Return the name of the quorum.
     */
    public String getName()
    {
        return name;
    }
}