#ifndef SHARDSERVER_H
#define SHARDSERVER_H

#include "Application/Common/ClusterContext.h"
#include "System/Config.h"

/*
===============================================================================================

 ShardServer

===============================================================================================
*/

class ShardServer : public ClusterContext
{
public:
    void Init();
    
    // For ConfigContext
    void            OnAppend(ConfigMessage& message, bool ownAppend);

    // ========================================================================================
    // ClusterContext interface:
    //
    void            OnClusterMessage(uint64_t nodeID, ClusterMessage& msg);
    void            OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint);
    void            OnAwaitingNodeID(Endpoint endpoint);
    // ========================================================================================

private:
    bool            awaitingNodeID;
};

#endif
