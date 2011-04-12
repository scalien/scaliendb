#ifndef SHARDQUORUMPROCESSOR_H
#define SHARDQUORUMPROCESSOR_H

#include "Application/Common/ClusterMessage.h"
#include "Application/Common/ClientRequest.h"
#include "ShardMessage.h"
#include "ShardQuorumContext.h"
#include "ShardCatchupReader.h"
#include "ShardCatchupWriter.h"

class ShardServer;

#define NORMAL_PRIMARYLEASE_REQUEST_TIMEOUT         500
#define ACTIVATION_PRIMARYLEASE_REQUEST_TIMEOUT     100
#define MAX_LEASE_REQUESTS                          50

/*
===============================================================================================

 ShardRequestLease

===============================================================================================
*/

struct ShardLeaseRequest
{
    ShardLeaseRequest();
    
    ShardLeaseRequest*  prev;
    ShardLeaseRequest*  next;
    
    uint64_t            proposalID;
    uint64_t            expireTime;
};

/*
===============================================================================================

 ShardQuorumProcessor

===============================================================================================
*/

class ShardQuorumProcessor
{
    typedef InList<ShardMessage>        MessageList;
    typedef InList<ClientRequest>       RequestList;
    typedef InList<ShardLeaseRequest>   LeaseRequestList;

public:
    typedef SortedList<uint64_t>    ShardList;
    
    ShardQuorumProcessor();

    void                    Init(ConfigQuorum* configQuorum, ShardServer* shardServer);
    void                    Shutdown();

    ShardServer*            GetShardServer();

    bool                    IsPrimary();
    uint64_t                GetQuorumID();
    uint64_t                GetPaxosID();
    void                    SetPaxosID(uint64_t paxosID);
    ConfigQuorum*           GetConfigQuorum();
    
    // ========================================================================================
    // For ShardServer:
    //
    void                    OnReceiveLease(ClusterMessage& message);
    void                    OnClientRequest(ClientRequest* request);
    void                    OnClientClose(ClientSession* session);
    void                    SetActiveNodes(SortedList<uint64_t>& activeNodes);
    void                    RegisterPaxosID(uint64_t paxosID);
    void                    TryReplicationCatchup();
    void                    TrySplitShard(uint64_t parentShardID, uint64_t shardID,
                             ReadBuffer& splitKey);
    void                    TryTruncateTable(uint64_t tableID, uint64_t newShardID);
    void                    OnActivation();
    void                    OnActivationTimeout();

    void                    StopReplication();
    void                    ContinueReplication();

    bool                    IsCatchupActive();
    uint64_t                GetCatchupBytesSent();
    uint64_t                GetCatchupBytesTotal();
    uint64_t                GetCatchupThroughput();

//    bool                    IsShardMigrationActive();
    uint64_t                GetMigrateShardID();
    void                    OnShardMigrationClusterMessage(uint64_t nodeID, ClusterMessage& message);
    
    // ========================================================================================
    // For ShardQuorumContext:
    //
    void                    OnAppend(uint64_t paxosID, ReadBuffer& value, bool ownAppend);
    void                    OnStartCatchup();
    void                    OnCatchupMessage(CatchupMessage& message);
    bool                    IsPaxosBlocked();
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
    void                    OnResumeAppend();
    void                    LocalExecute();

    bool                    isPrimary;
    uint64_t                highestProposalID;
    uint64_t                configID;

    // for async OnAppend():
    bool                    ownAppend;
    uint64_t                paxosID;
    uint64_t                commandID;
    Buffer                  valueBuffer;
    ReadBuffer              value;

    ShardServer*            shardServer;
    ShardQuorumContext      quorumContext;

    LeaseRequestList        leaseRequests;
    MessageList             shardMessages;
    RequestList             clientRequests;
    
    uint64_t                migrateNodeID;
    uint64_t                migrateShardID;
    int64_t                 migrateCache; // in bytes
    
    ShardCatchupReader      catchupReader;
    ShardCatchupWriter      catchupWriter;

    Countdown               requestLeaseTimeout;
    Countdown               activationTimeout;
    Timer                   leaseTimeout;
    YieldTimer              tryAppend;
    YieldTimer              resumeAppend;
    YieldTimer              localExecute;
};

#endif
