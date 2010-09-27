#ifndef SHARDSERVER_H
#define SHARDSERVER_H

#include "System/Containers/InList.h"
#include "System/Containers/InSortedList.h"
#include "System/Events/Timer.h"
#include "System/Events/Countdown.h"
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
    typedef InList<QuorumData> QuorumList;

    void            Init();
    
    // For ConfigContext
    bool            IsLeaderKnown(uint64_t quorumID);
    bool            IsLeader(uint64_t quorumID);
    uint64_t        GetLeader(uint64_t quorumID);
    void            OnAppend(DataMessage& message, bool ownAppend);

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
    void            OnSetConfigState(ConfigState* configState);
    void            UpdateQuorumShards(QuorumData* qdata, List<uint64_t>& shards);
    
    bool            awaitingNodeID;
    QuorumList      quorums;
    ConfigState*    configState;
    Countdown       requestTimer;
    Timer           primaryLeaseTimer;
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

    QuorumData()    { isPrimary = false; prev = next = this; }

    bool            isPrimary;
    uint64_t        primaryExpireTime;

    uint64_t        quorumID;
    MessageList     dataMessages;
    RequestList     requests;
    DataContext     context;
    
    QuorumData*     prev;
    QuorumData*     next;
};

#endif
