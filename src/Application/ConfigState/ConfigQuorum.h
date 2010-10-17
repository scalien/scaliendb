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
    
    ConfigQuorum&       operator=(const ConfigQuorum& other);

    uint64_t            quorumID;
    NodeList            activeNodes;
    NodeList            inactiveNodes;
    List<uint64_t>      shards;
    
    // ========================================================================================
    //
    // Not replicated, only stored by the MASTER in-memory
    bool                isActivatingNode;
    uint64_t            activatingNodeID;
    
    bool                hasPrimary;
    uint64_t            primaryID;
    uint64_t            paxosID;
    //
    // ========================================================================================

    ConfigQuorum*       prev;
    ConfigQuorum*       next;
};


inline ConfigQuorum::ConfigQuorum()
{
    prev = next = this;
    quorumID = 0;
    isActivatingNode = false;
    activatingNodeID = 0;
    hasPrimary = false;
    primaryID = 0;
    paxosID = 0;
}

inline ConfigQuorum::ConfigQuorum(const ConfigQuorum& other)
{
    *this = other;
}

inline ConfigQuorum& ConfigQuorum::operator=(const ConfigQuorum& other)
{
    uint64_t*   sit;
    
    quorumID = other.quorumID;
    activeNodes = other.activeNodes;
    inactiveNodes = other.inactiveNodes;
    
    for (sit = other.shards.First(); sit != NULL; sit = other.shards.Next(sit))
        shards.Append(*sit);
    
    hasPrimary = other.hasPrimary;
    primaryID = other.primaryID;
    paxosID = other.paxosID;
    
    prev = next = this;
    
    return *this;
}

#endif
