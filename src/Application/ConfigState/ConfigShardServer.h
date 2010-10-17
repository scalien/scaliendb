#ifndef CONFIGSHARDSERVER_H
#define CONFIGSHARDSERVER_H

#include "System/Common.h"
#include "System/Buffers/Buffer.h"
#include "System/Containers/ArrayList.h"
#include "System/IO/Endpoint.h"

#define MAX_QUORUMS_PER_SHARDSERVER  1000

/*
===============================================================================================

 QuorumPaxosID

===============================================================================================
*/

class QuorumPaxosID
{
public:
    QuorumPaxosID();
    
    uint64_t        quorumID;
    uint64_t        paxosID;
    
    typedef ArrayList<QuorumPaxosID, MAX_QUORUMS_PER_SHARDSERVER> List;

    static bool     ReadList(ReadBuffer& buffer, List& quorumPaxosIDs);
    static bool     WriteList(Buffer& buffer, List& quorumPaxosIDs);
    
    static uint64_t GetPaxosID(List& quorumPaxosIDs, uint64_t quorumID);
};


/*
===============================================================================================

 ConfigShardServer

===============================================================================================
*/

class ConfigShardServer
{
public:
    ConfigShardServer();
    ConfigShardServer(const ConfigShardServer& other);
    
    ConfigShardServer&  operator=(const ConfigShardServer& other);

    uint64_t            nodeID;
    Endpoint            endpoint;
    
    // ========================================================================================
    //
    // Not replicated, only stored by the MASTER in-memory
    QuorumPaxosID::List quorumPaxosIDs;
    
    uint64_t            nextActivationTime;
    // ========================================================================================
    
    ConfigShardServer*  prev;
    ConfigShardServer*  next;
};

#endif
