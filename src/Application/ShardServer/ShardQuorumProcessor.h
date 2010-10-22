#ifndef SHARDQUORUMPROCESSOR_H
#define SHARDQUORUMPROCESSOR_H

#include "Application/Common/ClusterMessage.h"
#include "Application/Common/ClientRequest.h"
#include "ShardMessage.h"
#include "ShardQuorumContext.h"
#include "CatchupReader.h"
#include "CatchupWriter.h"

class ShardServer;

#define PRIMARYLEASE_REQUEST_TIMEOUT     1000

/*
===============================================================================================

 ShardQuorumProcessor

===============================================================================================
*/

class ShardQuorumProcessor
{
    typedef InList<ShardMessage>    MessageList;
    typedef InList<ClientRequest>   RequestList;

public:
    typedef List<uint64_t>          ShardList;
    
    ShardQuorumProcessor();

    void                    Init(ConfigQuorum* configQuorum, ShardServer* shardServer);
    ShardServer*            GetShardServer();

    bool                    IsPrimary();
    uint64_t                GetQuorumID();
    uint64_t                GetPaxosID();
    ShardList&              GetShards();
    
    // ========================================================================================
    // For ShardServer:
    //
    void                    OnReceiveLease(ClusterMessage& message);
    void                    OnClientRequest(ClientRequest* request);
    void                    OnClientClose(ClientSession* session);
    void                    SetActiveNodes(ConfigQuorum::NodeList& activeNodes);
    void                    TryReplicationCatchup();
    
    // ========================================================================================
    // For ShardQuorum:
    //
    void                    OnAppend(ReadBuffer& value, bool ownAppend);
    void                    OnStartCatchup();
    void                    OnCatchupMessage(CatchupMessage& message);
    // ========================================================================================

    void                    OnRequestLeaseTimeout();
    void                    OnLeaseTimeout();

    ShardQuorumProcessor*   prev;
    ShardQuorumProcessor*   next;

private:
    void                    OnClientRequestGet(ClientRequest* request);

    void                    TransformRequest(ClientRequest* request, ShardMessage* message);
    void                    ProcessMessage(ShardMessage& message, bool ownAppend);
    void                    TryAppend();

    bool                    isPrimary;
//    bool                    isCatchingUp;
    uint64_t                proposalID;
    uint64_t                configID;
    uint64_t                requestedLeaseExpireTime;

    ShardServer*            shardServer;
    ShardQuorumContext      quorumContext;

    MessageList             shardMessages;
    RequestList             clientRequests;
    ShardList               shards;
    ConfigQuorum::NodeList  activeNodes;
    
    CatchupReader           catchupReader;
    CatchupWriter           catchupWriter;

    Countdown               requestLeaseTimeout;
    Timer                   leaseTimeout;
};

#endif
