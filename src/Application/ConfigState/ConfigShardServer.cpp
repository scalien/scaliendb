#include "ConfigShardServer.h"

QuorumInfo::QuorumInfo()
{
    quorumID = 0;
    paxosID = 0;
    needCatchup = false;
    unused1 = 0;
    unused2 = 0;
    unused3 = 0;
}

bool QuorumInfo::ReadList(ReadBuffer& buffer, List<QuorumInfo>& quorumInfos)
{
    unsigned        i, length;
    int             read;
    QuorumInfo      quorumInfo;
    
    read = buffer.Readf("%u", &length);
    if (read < 1)
        return false;
    buffer.Advance(read);
    for (i = 0; i < length; i++)
    {
        read = buffer.Readf(":%U:%U:%b:%U:%U:%U",
         &quorumInfo.quorumID, &quorumInfo.paxosID,
         &quorumInfo.needCatchup, &quorumInfo.unused1,
         &quorumInfo.unused2, &quorumInfo.unused3);
        if (read < 4)
            return false;
        buffer.Advance(read);
        quorumInfos.Append(quorumInfo);
    }
    return true;
}

bool QuorumInfo::WriteList(Buffer& buffer, List<QuorumInfo>& quorumInfos)
{
    QuorumInfo*  it;

    buffer.Appendf("%u", quorumInfos.GetLength());
    FOREACH (it, quorumInfos)
    {
        buffer.Appendf(":%U:%U:%b:%U:%U:%U",
         it->quorumID, it->paxosID,
         it->needCatchup, it->unused1,
         it->unused2, it->unused3);
    }
    
    return true;
}

QuorumInfo* QuorumInfo::GetQuorumInfo(List<QuorumInfo>& quorumInfos, uint64_t quorumID)
{
    QuorumInfo*  it;

    FOREACH (it, quorumInfos)
    {
        if (it->quorumID == quorumID)
            return it;
    }
    
    return NULL;
}

QuorumShardInfo::QuorumShardInfo()
{
    quorumID = 0;
    shardID = 0;
    shardSize = 0;
    isSendingShard = false;
    migrationNodeID = 0;
    migrationQuorumID = 0;
    migrationBytesSent = 0;
    migrationBytesTotal = 0;
    migrationThroughput = 0;
}

bool QuorumShardInfo::ReadList(ReadBuffer& buffer, List<QuorumShardInfo>& quorumShardInfos)
{
    unsigned        i, length;
    int             read;
    QuorumShardInfo quorumShardInfo;
    
    read = buffer.Readf("%u", &length);
    if (read < 1)
        return false;
    buffer.Advance(read);
    for (i = 0; i < length; i++)
    {
        read = buffer.Readf(":%U:%U:%U:%#B:%b:%b:%U:%U:%U:%U:%U",
         &quorumShardInfo.quorumID, &quorumShardInfo.shardID,
         &quorumShardInfo.shardSize, &quorumShardInfo.splitKey, &quorumShardInfo.isSplitable,
         &quorumShardInfo.isSendingShard, &quorumShardInfo.migrationQuorumID,
         &quorumShardInfo.migrationNodeID, &quorumShardInfo.migrationBytesSent,
         &quorumShardInfo.migrationBytesTotal, &quorumShardInfo.migrationThroughput);
        if (read < 6)
            return false;
        buffer.Advance(read);
        quorumShardInfos.Append(quorumShardInfo);
    }
    return true;
}

bool QuorumShardInfo::WriteList(Buffer& buffer, List<QuorumShardInfo>& quorumShardInfos)
{
    QuorumShardInfo*  it;

    buffer.Appendf("%u", quorumShardInfos.GetLength());
    FOREACH (it, quorumShardInfos)
    {
        buffer.Appendf(":%U:%U:%U:%#B:%b:%b:%U:%U:%U:%U:%U",
         it->quorumID, it->shardID, it->shardSize, &it->splitKey, it->isSplitable,
         it->isSendingShard, it->migrationQuorumID,
         it->migrationNodeID, it->migrationBytesSent,
         it->migrationBytesTotal, it->migrationThroughput);
    }
    
    return true;
}

QuorumPriority::QuorumPriority()
{
    quorumID = 0;
    priority = 0;
}

ConfigShardServer::ConfigShardServer()
{
    prev = next = this;
    nodeID = 0;
    activationPhase = 0;
    nextActivationTime = 0;
    httpPort = 0;
    sdbpPort = 0;
    hasHeartbeat = false;
}

ConfigShardServer::ConfigShardServer(const ConfigShardServer& other)
{
    *this = other;
}

ConfigShardServer& ConfigShardServer::operator=(const ConfigShardServer& other)
{
    nodeID = other.nodeID;
    activationPhase = other.activationPhase;
    nextActivationTime = other.nextActivationTime;
    endpoint = other.endpoint;
    httpPort = other.httpPort;
    sdbpPort = other.sdbpPort;
    hasHeartbeat = other.hasHeartbeat;
    
    quorumInfos = other.quorumInfos;
    quorumShardInfos = other.quorumShardInfos;
    quorumPriorities = other.quorumPriorities;

    prev = next = this;
    
    return *this;
}

uint64_t ConfigShardServer::GetQuorumPriority(uint64_t quorumID)
{
    QuorumPriority* quorumPriority;

    FOREACH(quorumPriority, quorumPriorities)
    {
        if (quorumPriority->quorumID == quorumID)
            return quorumPriority->priority;
    }

    return 1;
}
