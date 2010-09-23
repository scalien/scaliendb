#ifndef SHARDSERVER_H
#define SHARDSERVER_H

#include "System/Containers/InList.h"
#include "System/Containers/InSortedList.h"
#include "System/Events/Timer.h"
#include "Application/Common/ClusterContext.h"
#include "Application/Common/ClientRequest.h"
#include "Application/SDBP/SDBPContext.h"
#include "Application/Controller/State/ConfigState.h" // TODO: move this to Application/Common
#include "DataMessage.h"

class QuorumData; // forward

/*
===============================================================================================

 ShardServer

===============================================================================================
*/

class ShardServer : public ClusterContext, public SDBPContext
{
public:
    typedef InList<QuorumData>      QuorumList;
    void Init();
    
    // For ConfigContext
    void            OnAppend(ConfigMessage& message, bool ownAppend);

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
    
    bool            awaitingNodeID;
    QuorumList      quorums;
    ConfigState     configState;
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

    MessageList     dataMessages;
    Timer           primaryLeaseTimeout;
    RequestList     requests;
    
    QuorumData*     prev;
    QuorumData*     next;
};

#endif
