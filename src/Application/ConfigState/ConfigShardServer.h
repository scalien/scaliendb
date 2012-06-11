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
    uint64_t            catchupBytesSent;
    uint64_t            catchupBytesTotal;
    uint64_t            catchupThroughput;
        
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

    bool                isSendingShard;
    uint64_t            migrationQuorumID;
    uint64_t            migrationNodeID;
    uint64_t            migrationBytesSent;
    uint64_t            migrationBytesTotal;
    uint64_t            migrationThroughput;
            
    static bool         ReadList(ReadBuffer& buffer, List<QuorumShardInfo>& quorumShardInfos);
    static bool         WriteList(Buffer& buffer, List<QuorumShardInfo>& quorumShardInfos);
};

/*
===============================================================================================

 QuorumPriority

===============================================================================================
*/

class QuorumPriority
{
public:
    QuorumPriority();

    uint64_t                quorumID;
    uint64_t                priority;
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
    uint64_t                GetQuorumPriority(uint64_t quorumID);

    uint64_t                nodeID;
    Endpoint                endpoint;
    List<QuorumPriority>    quorumPriorities;
    
    // ========================================================================================
    //
    // Not replicated, only stored by the MASTER in-memory
    int                     activationPhase;
    uint64_t                nextActivationTime;
    List<QuorumInfo>        quorumInfos;
    List<QuorumShardInfo>   quorumShardInfos;
    
    unsigned                httpPort;
    unsigned                sdbpPort;
    bool                    hasHeartbeat;
    // ========================================================================================
    
    ConfigShardServer*      prev;
    ConfigShardServer*      next;
};

#endif
