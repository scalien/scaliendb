#ifndef CLUSTERCONTEXT_H
#define CLUSTERCONTEXT_H

#include "System/IO/Endpoint.h"
#include "ClusterMessage.h"

/*
===============================================================================================

 ClusterContext

===============================================================================================
*/

class ClusterContext
{
public:
    virtual ~ClusterContext() {}
    
    virtual void OnClusterMessage(uint64_t nodeID, ClusterMessage& msg)             = 0;
    virtual void OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint)      = 0;
    virtual void OnAwaitingNodeID(Endpoint endpoint)                                = 0;
};

#endif
