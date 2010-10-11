#ifndef SDBPCLIENTSHARDCONNECTION_H
#define SDBPCLIENTSHARDCONNECTION_H

#include "System/Containers/InTreeMap.h"
#include "System/Containers/SortedList.h"
#include "System/Containers/InList.h"
#include "System/IO/Endpoint.h"
#include "Framework/Messaging/MessageConnection.h"
#include "Application/Common/ClientResponse.h"
#include "SDBPClientRequest.h"

namespace SDBPClient
{

class Client;   // forward

/*
===============================================================================================

 SDBPClient::ShardConnection

===============================================================================================
*/

class ShardConnection : public MessageConnection
{
public:
    typedef InTreeNode<ShardConnection> TreeNode;

    ShardConnection(Client* client, uint64_t nodeID, Endpoint& endpoint);
    
    void                    Connect();
    bool                    SendRequest(Request* request);
    void                    SendSubmit();

    uint64_t                GetNodeID();
    Endpoint&               GetEndpoint();
    bool                    IsWritePending();

    void                    SetQuorumMembership(uint64_t quorumID);
    void                    ClearQuorumMembership(uint64_t quorumID);
    void                    ClearQuorumMemberships();
    SortedList<uint64_t>&   GetQuorumList();
    
    // MessageConnection interface
    virtual bool            OnMessage(ReadBuffer& msg);
    virtual void            OnWrite();
    virtual void            OnConnect();
    virtual void            OnClose();

    TreeNode                treeNode;

private:
    void                    InvalidateQuorum(uint64_t quorumID);
    void                    SendQuorumRequests();

    Client*                 client;
    uint64_t                nodeID;
    Endpoint                endpoint;
    SortedList<uint64_t>    quorums;
    InList<Request>         sentRequests;
    Buffer*                 writeBuffer;
};

}; // namespace

#endif
