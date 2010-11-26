#ifndef CONFIGSHARDSERVER_H
#define CONFIGSHARDSERVER_H

#include "System/Common.h"
#include "System/Buffers/Buffer.h"
#include "System/Containers/List.h"
#include "System/IO/Endpoint.h"

/*
===============================================================================================

 QuorumPaxosID

===============================================================================================
*/

class QuorumPaxosID
{
public:
    typedef List<QuorumPaxosID> List;

    QuorumPaxosID();
    
    uint64_t        quorumID;
    uint64_t        paxosID;
    
    static bool     ReadList(ReadBuffer& buffer, List& quorumPaxosIDs);
    static bool     WriteList(Buffer& buffer, List& quorumPaxosIDs);
    
    static uint64_t GetPaxosID(List& quorumPaxosIDs, uint64_t quorumID);
};

/*
===============================================================================================

 QuorumShardInfo

===============================================================================================
*/

class QuorumShardInfo
{
public:
    typedef List<QuorumShardInfo> List;

    QuorumShardInfo();
    
    uint64_t        quorumID;
    uint64_t        shardID;
    uint64_t        shardSize;
    ReadBuffer      splitKey;
        
    static bool     ReadList(ReadBuffer& buffer, List& quorumShardInfos);
    static bool     WriteList(Buffer& buffer, List& quorumShardInfos);
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
    
    ConfigShardServer&      operator=(const ConfigShardServer& other);

    uint64_t                nodeID;
    Endpoint                endpoint;
    
    // ========================================================================================
    //
    // Not replicated, only stored by the MASTER in-memory
    List<QuorumPaxosID>     quorumPaxosIDs;
    List<QuorumShardInfo>   quorumShardInfos;
    
    uint64_t                nextActivationTime;

    unsigned                httpPort;
    unsigned                sdbpPort;
    // ========================================================================================
    
    ConfigShardServer*      prev;
    ConfigShardServer*      next;
};

#endif
