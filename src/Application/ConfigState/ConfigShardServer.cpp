#include "ConfigShardServer.h"

QuorumPaxosID::QuorumPaxosID()
{
    quorumID = 0;
    paxosID = 0;
}

bool QuorumPaxosID::ReadList(ReadBuffer& buffer, List<QuorumPaxosID>& quorumPaxosIDs)
{
    unsigned        i, length;
    int             read;
    QuorumPaxosID   quorumPaxosID;
    
    read = buffer.Readf("%u", &length);
    if (read < 1)
        return false;
    buffer.Advance(read);
    for (i = 0; i < length; i++)
    {
        read = buffer.Readf(":%U:%U", &quorumPaxosID.quorumID, &quorumPaxosID.paxosID);
        if (read < 4)
            return false;
        buffer.Advance(read);
        quorumPaxosIDs.Append(quorumPaxosID);
    }
    return true;
}

bool QuorumPaxosID::WriteList(Buffer& buffer, List<QuorumPaxosID>& quorumPaxosIDs)
{
    QuorumPaxosID*  it;

    buffer.Appendf("%u", quorumPaxosIDs.GetLength());
    FOREACH(it, quorumPaxosIDs)
    {
        buffer.Appendf(":%U:%U", it->quorumID, it->paxosID);
    }
    
    return true;
}

uint64_t QuorumPaxosID::GetPaxosID(List<QuorumPaxosID>& quorumPaxosIDs, uint64_t quorumID)
{
    QuorumPaxosID*  it;

    FOREACH(it, quorumPaxosIDs)
    {
        if (it->quorumID == quorumID)
            return it->paxosID;
    }
    
    return 0;
}

QuorumShardInfo::QuorumShardInfo()
{
    quorumID = 0;
    shardID = 0;
    shardSize = 0;
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
        read = buffer.Readf(":%U:%U:%U:%#R",
         &quorumShardInfo.quorumID, &quorumShardInfo.shardID,
         &quorumShardInfo.shardSize, &quorumShardInfo.splitKey);
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
    FOREACH(it, quorumShardInfos)
    {
        buffer.Appendf(":%U:%U:%U:%#R", it->quorumID, it->shardID, it->shardSize, &it->splitKey);
    }
    
    return true;
}

ConfigShardServer::ConfigShardServer()
{
    prev = next = this;
    nextActivationTime = 0;
    httpPort = 0;
    sdbpPort = 0;
}

ConfigShardServer::ConfigShardServer(const ConfigShardServer& other)
{
    *this = other;
}

ConfigShardServer& ConfigShardServer::operator=(const ConfigShardServer& other)
{
    nodeID = other.nodeID;
    endpoint = other.endpoint;
    httpPort = other.httpPort;
    sdbpPort = other.sdbpPort;

    prev = next = this;
    
    return *this;
}

