package com.scalien.scaliendb;

import java.math.BigInteger;

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
    
    /**
     * Returns the ID of the quorum.
     */
    long getQuorumID() {
        return quorumID;
    }

    /**
     * The name of the quorum.
     */
    public String getName()
    {
        return name;
    }
}