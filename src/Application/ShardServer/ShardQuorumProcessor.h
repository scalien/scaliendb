#ifndef SHARDQUORUMPROCESSOR_H
#define SHARDQUORUMPROCESSOR_H

#include "System/Containers/InCache.h"
#include "Application/Common/ClusterMessage.h"
#include "Application/Common/ClientRequest.h"
#include "ShardMessage.h"
#include "ShardQuorumContext.h"
#include "ShardCatchupReader.h"
#include "ShardCatchupWriter.h"

class ShardServer;

#define NORMAL_PRIMARYLEASE_REQUEST_TIMEOUT         (500)
#define ACTIVATION_PRIMARYLEASE_REQUEST_TIMEOUT     (100)
#define MAX_LEASE_REQUESTS                          (50)
#define UNBLOCK_SHARD_TIMEOUT                       (3000)
#define SEQUENCE_GRANULARITY                        (1000)

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

 ShardAppendState

===============================================================================================
*/

struct ShardAppendState
{
    bool                    currentAppend;
    uint64_t                paxosID;
    uint64_t                commandID;
    Buffer                  valueBuffer;
    ReadBuffer              value;
    
    void                    Reset();
};

/*
===============================================================================================

 ShardQuorumProcessor

===============================================================================================
*/

class ShardQuorumProcessor
{
    typedef InCache<ShardMessage>       MessageCache;
    typedef InList<ShardMessage>        MessageList;
    typedef InList<ClientRequest>       RequestList;
    typedef InList<ShardLeaseRequest>   LeaseRequestList;

public:
    typedef SortedList<uint64_t>    ShardList;
    
    ShardQuorumProcessor();
    ~ShardQuorumProcessor();

    void                    Init(ConfigQuorum* configQuorum, ShardServer* shardServer);
    void                    Shutdown();

    ShardServer*            GetShardServer();

    bool                    IsPrimary();
    uint64_t                GetQuorumID();
    uint64_t                GetPaxosID();
    void                    SetPaxosID(uint64_t paxosID);
    uint64_t                GetLastLearnChosenTime();
    ConfigQuorum*           GetConfigQuorum();
    
    // ========================================================================================
    // For ShardServer:
    //
    void                    OnSetConfigState();
    void                    OnReceiveLease(uint64_t nodeID, ClusterMessage& message);
    void                    OnClientRequest(ClientRequest* request);
    void                    OnClientClose(ClientSession* session);
    void                    RegisterPaxosID(uint64_t paxosID);
    void                    TryReplicationCatchup();
    void                    TrySplitShard(uint64_t parentShardID, uint64_t shardID,
                             ReadBuffer& splitKey);
    void                    TryTruncateTable(uint64_t tableID, uint64_t newShardID);
    void                    OnActivation();
    void                    OnActivationTimeout();

    void                    StopReplication();
    void                    ContinueReplication();

    // Catchup sending:
    bool                    IsCatchupActive();
    void                    AbortCatchup();
    uint64_t                GetCatchupBytesSent();
    uint64_t                GetCatchupBytesTotal();
    uint64_t                GetCatchupThroughput();

    uint64_t                GetMigrateShardID();
    void                    OnShardMigrationClusterMessage(uint64_t nodeID, ClusterMessage& message);
    void                    SetBlockReplication(bool blockReplication);
    void                    SetReplicationLimit(unsigned replicationLimit);
    
    uint64_t                GetMessageCacheSize();
    uint64_t                GetMessageListSize();
    uint64_t                GetShardAppendStateSize();
    uint64_t                GetQuorumContextSize();

    // ========================================================================================
    // For ShardQuorumContext:
    //
    void                    OnStartProposing();
    void                    OnAppend(uint64_t paxosID, Buffer& value, bool ownAppend);
    void                    OnStartCatchup();
    void                    OnCatchupMessage(CatchupMessage& message);
    bool                    IsPaxosBlocked();
    // ========================================================================================

    void                    OnRequestLeaseTimeout();
    void                    OnLeaseTimeout();

    bool                    IsResumeAppendActive();

    ShardQuorumProcessor*   prev;
    ShardQuorumProcessor*   next;

private:
    void                    TransformRequest(ClientRequest* request, ShardMessage* message);
    void                    ExecuteMessage(uint64_t paxosID, uint64_t commandID,
                             ShardMessage* message, bool ownCommand);
    void                    TryAppend();
    void                    OnResumeAppend();
    void                    OnResumeBlockedAppend();
    void                    StartTransaction(ClientRequest* request);
    void                    CommitTransaction(ClientRequest* request);
    void                    RollbackTransaction(ClientRequest* request);

    bool                    isPrimary;
    uint64_t                highestProposalID;
    uint64_t                configID;
    uint64_t                prevAppendTime;
    unsigned                appendDelay;

    ShardAppendState        appendState;

    ShardServer*            shardServer;
    ShardQuorumContext      quorumContext;

    SortedList<uint64_t>    activeNodes;
    LeaseRequestList        leaseRequests;
    MessageCache            messageCache;
    MessageList             shardMessages;
    
    uint64_t                migrateNodeID;
    uint64_t                migrateShardID;
    int64_t                 migrateCache; // in bytes
    bool                    blockReplication;
    
    ShardCatchupReader      catchupReader;
    ShardCatchupWriter      catchupWriter;

    Countdown               requestLeaseTimeout;
    Countdown               activationTimeout;
    Timer                   leaseTimeout;
    Timer                   tryAppend;
    YieldTimer              resumeAppend;
    Countdown               resumeBlockedAppend;
};

#endif
