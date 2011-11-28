#ifndef SDBPSHARDCONNECTION_H
#define SDBPSHARDCONNECTION_H

#include "System/Containers/InTreeMap.h"
#include "System/Containers/SortedList.h"
#include "System/Containers/InList.h"
#include "System/IO/Endpoint.h"
#include "Application/Common/ClientResponse.h"
#include "SDBPClientRequest.h"

namespace SDBPClient
{

class Client;   // forward
class PooledShardConnection;

/*
===============================================================================================

 SDBPClient::ShardConnection

===============================================================================================
*/

class ShardConnection
{
public:
    typedef InTreeNode<ShardConnection> TreeNode;

    ShardConnection(Client* client, uint64_t nodeID, Endpoint& endpoint);
    
    void                    ClearRequests();
    bool                    SendRequest(Request* request);
    void                    Flush();
    void                    ReleaseConnection();

    uint64_t                GetNodeID() const;
    Endpoint&               GetEndpoint();
    bool                    IsWritePending();
    bool                    IsConnected();
    unsigned                GetNumSentRequests();

    void                    SetQuorumMembership(uint64_t quorumID);
    void                    ClearQuorumMembership(uint64_t quorumID);
    void                    ClearQuorumMemberships();
    SortedList<uint64_t>&   GetQuorumList();
    
    void                    ReassignSentRequests();
    
    // MessageConnection interface
    bool                    OnMessage(ReadBuffer& msg);
    void                    OnWrite();
    void                    OnConnect();
    void                    OnClose();

    TreeNode                treeNode;

private:
    void                    InvalidateQuorum(uint64_t quorumID);
    void                    SendQuorumRequests();

    Client*                 client;
    PooledShardConnection*  conn;
    uint64_t                nodeID;
    Endpoint                endpoint;
    SortedList<uint64_t>    quorums;
    InList<Request>         sentRequests;
    ClientResponse          response;
};

}; // namespace

#endif
