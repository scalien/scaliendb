#ifndef SHARDSERVER_H
#define SHARDSERVER_H

#include "System/Containers/InList.h"
#include "System/Containers/InSortedList.h"
#include "System/Containers/HashMap.h"
#include "System/Events/Timer.h"
#include "System/Events/Countdown.h"
#include "Framework/Storage/StorageEnvironment.h"
#include "Framework/Storage/StorageDatabase.h"
#include "Framework/Storage/StorageTable.h"
#include "Framework/Storage/StorageCursor.h"
#include "Application/Common/ClusterContext.h"
#include "Application/Common/ClientRequest.h"
#include "Application/Common/CatchupMessage.h"
#include "Application/SDBP/SDBPContext.h"
#include "Application/ConfigState/ConfigState.h"
#include "DataMessage.h"
#include "DataContext.h"

class QuorumData; // forward

/*
===============================================================================================

 ShardServer

===============================================================================================
*/

class ShardServer : public ClusterContext, public SDBPContext
{
public:
    typedef InList<QuorumData>                      QuorumList;
    typedef ArrayList<uint64_t, CONFIG_MAX_NODES>   NodeList;
    typedef HashMap<uint64_t, StorageDatabase*>     DatabaseMap;
    typedef HashMap<uint64_t, StorageTable*>        TableMap;

    void                Init();
    void                Shutdown();

    QuorumList*         GetQuorums();
    StorageEnvironment& GetEnvironment();
    void                ProcessMessage(QuorumData* quorumData, DataMessage& message, bool ownAppend);
    void                OnWriteReadyness();
    
    // ========================================================================================
    // For DataContext:
    //
    bool                IsLeaseKnown(uint64_t quorumID);
    bool                IsLeaseOwner(uint64_t quorumID);
    uint64_t            GetLeaseOwner(uint64_t quorumID);
    void                OnAppend(uint64_t quorumID, ReadBuffer& value, bool ownAppend);
    void                OnStartCatchup(uint64_t quorumID);
    void                OnCatchupMessage(CatchupMessage& message);

    // ========================================================================================
    // SDBPContext interface:
    //
    bool                IsValidClientRequest(ClientRequest* request);
    void                OnClientRequest(ClientRequest* request);
    void                OnClientClose(ClientSession* session);

    // ========================================================================================
    // ClusterContext interface:
    //
    void                OnClusterMessage(uint64_t nodeID, ClusterMessage& msg);
    void                OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint);
    bool                OnAwaitingNodeID(Endpoint endpoint);
    // ========================================================================================

private:
    QuorumData*         LocateQuorum(uint64_t quorumID);
    void                TryAppend(QuorumData* quorumData);
    void                FromClientRequest(ClientRequest* request, DataMessage* message);
    void                OnRequestLeaseTimeout();
    void                OnPrimaryLeaseTimeout();
    void                OnSetConfigState(ConfigState& configState);
    void                ConfigureQuorum(ConfigQuorum* configQuorum, bool active);
    void                OnReceiveLease(uint64_t quorumID, uint64_t proposalID);
    void                UpdateStorageShards(List<uint64_t>& shards);
    StorageTable*       LocateTable(uint64_t tableID);
    StorageShard*       LocateShard(uint64_t shardID);
    StorageTable*       GetQuorumTable(uint64_t quorumID);
    void                UpdatePrimaryLeaseTimer();
    void                OnClientRequestGet(ClientRequest* request);
    
    // Catchup logic
    void                OnCatchupRequest(CatchupMessage& message);
    void                OnCatchupBeginShard(CatchupMessage& message);
    void                OnCatchupKeyValue(CatchupMessage& message);
    void                OnCatchupCommit(CatchupMessage& message);
    
    bool                isAwaitingNodeID;
    QuorumList          quorums;
    ConfigState         configState;
    Countdown           requestTimer;
    NodeList            controllers;
    StorageDatabase*    systemDatabase;
    StorageEnvironment  databaseEnv;
    DatabaseMap         databases;
    TableMap            tables;

    // isSendingCatchup & isCatchingUp is in ShardServer and not in QuorumData because a shard server
    // should only be doing catchup stuff one quorum at a time!
    bool                isSendingCatchup;   // whether I'm helping someone to catchup
    bool                isCatchingUp;       // whether I'm catching up
    StorageCursor*      catchupCursor;      // writer
    CatchupMessage      catchupRequest;     // writer
    uint64_t            catchupShardID;     // writer
    uint64_t            catchupPaxosID;     // writer
    StorageShard*       catchupShard;       // reader
    QuorumData*         catchupQuorum;      // reader
};

/*
===============================================================================================

 QuorumData

===============================================================================================
*/

class QuorumData
{
public:
    typedef InList<DataMessage>     MessageList;
    typedef InList<ClientRequest>   RequestList;
    typedef List<uint64_t>          ShardList;

    QuorumData();

    bool            isPrimary;
    bool            isActive;

    uint64_t        quorumID;
    uint64_t        proposalID;
    MessageList     dataMessages;
    RequestList     requests;
    ShardList       shards;
    DataContext     context;
    uint64_t        requestedLeaseExpireTime;
    Timer           leaseTimeout;
    
    QuorumData*     prev;
    QuorumData*     next;
};

#endif
