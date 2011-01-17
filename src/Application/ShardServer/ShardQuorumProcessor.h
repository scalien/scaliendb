#ifndef SHARDQUORUMPROCESSOR_H
#define SHARDQUORUMPROCESSOR_H

#include "Application/Common/ClusterMessage.h"
#include "Application/Common/ClientRequest.h"
#include "ShardMessage.h"
#include "ShardQuorumContext.h"
#include "ShardCatchupReader.h"
#include "ShardCatchupWriter.h"

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
    typedef SortedList<uint64_t>    ShardList;
    
    ShardQuorumProcessor();

    void                    Init(ConfigQuorum* configQuorum, ShardServer* shardServer);
    ShardServer*            GetShardServer();

    bool                    IsPrimary();
    uint64_t                GetQuorumID();
    uint64_t                GetPaxosID();
    ConfigQuorum*           GetConfigQuorum();
    
    // ========================================================================================
    // For ShardServer:
    //
    void                    OnReceiveLease(ClusterMessage& message);
    void                    OnClientRequest(ClientRequest* request);
    void                    OnClientClose(ClientSession* session);
    void                    SetActiveNodes(List<uint64_t>& activeNodes);
    void                    TryReplicationCatchup();
    void                    TrySplitShard(uint64_t parentShardID, uint64_t shardID,
                             ReadBuffer& splitKey);
    
    // ========================================================================================
    // For ShardQuorum:
    //
    void                    OnAppend(uint64_t paxosID, ReadBuffer& value, bool ownAppend);
    void                    OnStartCatchup();
    void                    OnCatchupMessage(CatchupMessage& message);
    // ========================================================================================

    void                    OnRequestLeaseTimeout();
    void                    OnLeaseTimeout();

    ShardQuorumProcessor*   prev;
    ShardQuorumProcessor*   next;

private:
    void                    TransformRequest(ClientRequest* request, ShardMessage* message);
    void                    ExecuteMessage(ShardMessage& message, uint64_t paxosID,
                             uint64_t commandID, bool ownAppend);
    void                    TryAppend();

    bool                    isPrimary;
    uint64_t                proposalID;
    uint64_t                configID;
    uint64_t                requestedLeaseExpireTime;
    int64_t                 shardMessagesLength;

    ShardServer*            shardServer;
    ShardQuorumContext      quorumContext;

    MessageList             shardMessages;
    RequestList             clientRequests;
    List<uint64_t>          activeNodes;
    
//    ShardCatchupReader      catchupReader;
//    ShardCatchupWriter      catchupWriter;

    Countdown               requestLeaseTimeout;
    Timer                   leaseTimeout;
    YieldTimer              tryAppend;
};

#endif
