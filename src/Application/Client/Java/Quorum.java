package com.scalien.scaliendb;

import java.math.BigInteger;

public class Quorum
{
    private Client client;
    private long quorumID;
    
    /**
     * Package constructor.
     *
     * @param   client  the client object
     * @param   name    the name of the database
     * @see     Client#getDatabase(String name)  getDatabase
     */
    Quorum(Client client, long quorumID) throws SDBPException {
        this.client = client;
        this.quorumID = quorumID;
    }
    
    /**
     * Returns the ID of the quorum.
     */
    public long getQuorumID() {
        return quorumID;
    }
    
    /**
     * Deletes the quorum.
     */
    public void deleteQuorum() throws SDBPException {
        client.deleteQuorum(quorumID);
    }

    /**
     * Adds node to the quorum.
     *
     * @param   nodeID  the ID of node to be added to the quorum
     */
    public void addNode(long nodeID) throws SDBPException {
        client.addNode(quorumID, nodeID);
    }

    /**
     * Removes node from the quorum.
     *
     * @param   nodeID  the ID of node to be removed to the quorum
     */
    public void removeNode(long nodeID) throws SDBPException {
        client.removeNode(quorumID, nodeID);
    }
}