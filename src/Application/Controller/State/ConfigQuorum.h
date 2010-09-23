#ifndef CONFIGQUORUM_H
#define CONFIGQUORUM_H

#include "System/Common.h"
#include "System/Containers/List.h"

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
    ConfigQuorum()      { prev = next = this; }

    uint64_t            quorumID;
    List<uint64_t>      activeNodes;    
    List<uint64_t>      inactiveNodes;  
    List<uint64_t>      shards;
    char                productionType;
    
    // ========================================================================================
    //
    // Not replicated, only stored by the MASTER in-memory
    bool                hasPrimary;
    uint64_t            primaryID;
    //
    // ========================================================================================

    ConfigQuorum*       prev;
    ConfigQuorum*       next;
};

#endif
