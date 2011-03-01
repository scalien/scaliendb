#ifndef CONFIGSHARDSERVER_H
#define CONFIGSHARDSERVER_H

#include "System/Common.h"
#include "System/Buffers/Buffer.h"
#include "System/Containers/List.h"
#include "System/IO/Endpoint.h"

/*
===============================================================================================

 QuorumInfo

===============================================================================================
*/

class QuorumInfo
{
public:
    QuorumInfo();
    
    uint64_t            quorumID;
    uint64_t            paxosID;

    bool                isSendingCatchup;
    uint64_t            bytesSent;
    uint64_t            bytesTotal;
    uint64_t            throughput;
    
    static bool         ReadList(ReadBuffer& buffer, List<QuorumInfo>& quorumInfos);
    static bool         WriteList(Buffer& buffer, List<QuorumInfo>& quorumInfos);
    
    static QuorumInfo*  GetQuorumInfo(List<QuorumInfo>& quorumInfos, uint64_t quorumID);
};

/*
===============================================================================================

 QuorumShardInfo

===============================================================================================
*/

class QuorumShardInfo
{
public:
    QuorumShardInfo();
    
    uint64_t            quorumID;
    uint64_t            shardID;
    uint64_t            shardSize;
    Buffer              splitKey;
    bool                isSplitable;
        
    static bool         ReadList(ReadBuffer& buffer, List<QuorumShardInfo>& quorumShardInfos);
    static bool         WriteList(Buffer& buffer, List<QuorumShardInfo>& quorumShardInfos);
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
    List<QuorumInfo>        quorumInfos;
    List<QuorumShardInfo>   quorumShardInfos;
    
    unsigned                httpPort;
    unsigned                sdbpPort;
    // ========================================================================================
    
    ConfigShardServer*      prev;
    ConfigShardServer*      next;
};

#endif
