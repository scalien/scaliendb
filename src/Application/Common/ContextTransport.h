#ifndef REPLICATIONTRANSPORT_H
#define REPLICATIONTRANSPORT_H

#include "System/Containers/HashMap.h"
#include "System/IO/Endpoint.h"
#include "Framework/Clustering/ClusterTransport.h"
#include "Framework/Replication/Quorums/QuorumContext.h"
#include "ClusterContext.h"

#define CONTEXT_TRANSPORT (ContextTransport::Get())

#define PROTOCOL_CLUSTER        'C'
#define PROTOCOL_QUORUM         'Q'

/*
===============================================================================================

 ContextTransport

===============================================================================================
*/

class ContextTransport : public ClusterTransport
{
    typedef HashMap<uint64_t, QuorumContext*> ContextMap;

public:
    /* Static instance */
    static ContextTransport* Get();

    void            Shutdown();

    void            SetClusterContext(ClusterContext* context);
    ClusterContext* GetClusterContext();

    void            AddQuorumContext(QuorumContext* context);
    void            RemoveQuorumContext(QuorumContext* context);
    QuorumContext*  GetQuorumContext(uint64_t quorumID);

    void            SendClusterMessage(uint64_t nodeID, Message& msg);
    void            SendQuorumMessage(uint64_t nodeID, uint64_t quorumID, Message& msg);
    
private:
    ContextTransport();

    // ========================================================================================
    // ClusterTransport interface:
    //
    void            OnConnectionReady(uint64_t nodeID, Endpoint endpoint);
    void            OnConnectionEnd(uint64_t nodeID, Endpoint endpoint);
    bool            OnAwaitingNodeID(Endpoint endpoint);
    void            OnMessage(uint64_t nodeID, ReadBuffer msg);
    // ========================================================================================

    void            OnClusterMessage(uint64_t nodeID, ReadBuffer& msg);
    void            OnQuorumMessage(uint64_t nodeID, ReadBuffer& msg);

    ClusterContext* clusterContext;
    ContextMap      contextMap;
};

#endif
