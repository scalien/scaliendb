#ifndef CONFIGQUORUM_H
#define CONFIGQUORUM_H

#include "System/Common.h"
#include "System/Containers/List.h"
#include "System/Containers/ArrayList.h"

// TODO: create ConfigConsts.h and define there
#define CONFIG_MAX_NODES                7

#define CONFIG_QUORUM_PRODUCTION        'P'
#define CONFIG_QUORUM_TEST              'T'

/*
===============================================================================================

 ConfigQuorum

===============================================================================================
*/

class ConfigQuorum
{
public:
    ConfigQuorum();
    ConfigQuorum(const ConfigQuorum& other);

    ConfigQuorum&       operator=(const ConfigQuorum& other);

    void                OnActivationStart(uint64_t nodeID, uint64_t expireTime);
    void                OnActivationMonitoring(uint64_t paxosID);
    void                OnActivationReplication();
    void                ClearActivation();

    List<uint64_t>      GetVolatileActiveNodes();
    
    uint64_t            quorumID;
    List<uint64_t>      activeNodes;
    List<uint64_t>      inactiveNodes;
    List<uint64_t>      shards;
    
    // ========================================================================================
    //
    // Not replicated, only stored by the MASTER in-memory
    bool                isActivatingNode;           // phase 1
    bool                isWatchingPaxosID;          // phase 2, not sent
    bool                isReplicatingActivation;    // phase 3, not sent
    uint64_t            configID;
    uint64_t            activatingNodeID;
    uint64_t            activationPaxosID;          // not sent
    uint64_t            activationExpireTime;       // not sent
    
    bool                hasPrimary;
    uint64_t            primaryID;
    uint64_t            paxosID;
    //
    // ========================================================================================

    ConfigQuorum*       prev;
    ConfigQuorum*       next;
};

#endif
