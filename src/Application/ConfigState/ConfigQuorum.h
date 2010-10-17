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
    typedef ArrayList<uint64_t, CONFIG_MAX_NODES> NodeList;

    ConfigQuorum();
    ConfigQuorum(const ConfigQuorum& other);

    NodeList            GetVolatileActiveNodes();
    
    ConfigQuorum&       operator=(const ConfigQuorum& other);

    uint64_t            quorumID;
    NodeList            activeNodes;
    NodeList            inactiveNodes;
    List<uint64_t>      shards;
    
    // ========================================================================================
    //
    // Not replicated, only stored by the MASTER in-memory
    uint64_t            configID;
    uint64_t            activatingNodeID;
    bool                isActivatingNode;           // phase 1
    bool                isWatchingPaxosID;          // phase 2, not sent
    bool                isReplicatingActivation;    // phase 3, not sent
    uint64_t            activationPaxosID;          // not sent
    
    bool                hasPrimary;
    uint64_t            primaryID;
    uint64_t            paxosID;
    //
    // ========================================================================================

    ConfigQuorum*       prev;
    ConfigQuorum*       next;
};

#endif
