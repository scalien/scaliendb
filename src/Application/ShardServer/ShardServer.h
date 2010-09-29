#ifndef SHARDSERVER_H
#define SHARDSERVER_H

#include "System/Containers/InList.h"
#include "System/Containers/InSortedList.h"
#include "System/Containers/HashMap.h"
#include "System/Events/Timer.h"
#include "System/Events/Countdown.h"
#include "Framework/Storage/StorageDatabase.h"
#include "Framework/Storage/StorageTable.h"
#include "Application/Common/ClusterContext.h"
#include "Application/Common/ClientRequest.h"
#include "Application/SDBP/SDBPContext.h"
#include "Application/Controller/State/ConfigState.h" // TODO: move this to Application/Common
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

    void            Init();
    
    // For ConfigContext
    bool            IsLeaderKnown(uint64_t quorumID);
    bool            IsLeader(uint64_t quorumID);
    uint64_t        GetLeader(uint64_t quorumID);
    void            OnAppend(uint64_t quorumID, DataMessage& message, bool ownAppend);

    // ========================================================================================
    // SDBPContext interface:
    //
    bool            IsValidClientRequest(ClientRequest* request);
    void            OnClientRequest(ClientRequest* request);
    void            OnClientClose(ClientSession* session);
    // ========================================================================================

    // ========================================================================================
    // ClusterContext interface:
    //
    void            OnClusterMessage(uint64_t nodeID, ClusterMessage& msg);
    void            OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint);
    void            OnAwaitingNodeID(Endpoint endpoint);
    // ========================================================================================

private:
    QuorumData*     LocateQuorum(uint64_t quorumID);
    void            TryAppend(QuorumData* quorumData);
    void            FromClientRequest(ClientRequest* request, DataMessage* message);
    void            OnRequestLeaseTimeout();
    void            OnPrimaryLeaseTimeout();
    void            OnSetConfigState(ConfigState* configState);
    void            OnReceiveLease(uint64_t quorumID, uint64_t proposalID);
    void            UpdateShards(List<uint64_t>& shards);
    StorageTable*   LocateTable(uint64_t tableID);
    StorageTable*   GetQuorumTable(uint64_t quorumID);
    void            UpdatePrimaryLeaseTimer();
    void            OnClientRequestGet(ClientRequest* request);
    
    bool            awaitingNodeID;
    QuorumList      quorums;
    ConfigState*    configState;
    Countdown       requestTimer;
    NodeList        controllers;
    StorageDatabase systemDatabase;
    DatabaseMap     databases;
    TableMap        tables;
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

    QuorumData()    { isPrimary = false; prev = next = this; }

    bool            isPrimary;
    uint64_t        primaryExpireTime;

    uint64_t        quorumID;
    uint64_t        proposalID;
    MessageList     dataMessages;
    RequestList     requests;
    ShardList       shards;
    DataContext     context;
    Timer           leaseTimeout;
    
    QuorumData*     prev;
    QuorumData*     next;
};

#endif
