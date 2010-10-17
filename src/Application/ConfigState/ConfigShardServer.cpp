#include "ConfigShardServer.h"

QuorumPaxosID::QuorumPaxosID()
{
    quorumID = 0;
    paxosID = 0;
}

bool QuorumPaxosID::ReadList(ReadBuffer& buffer, List& quorumPaxosIDs)
{
    unsigned        i, length;
    int             read;
    QuorumPaxosID   quorumPaxosID;
    
    read = buffer.Readf(":%u", &length);
    if (read < 2)
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

bool QuorumPaxosID::WriteList(Buffer& buffer, List& quorumPaxosIDs)
{
    QuorumPaxosID*  it;

    buffer.Appendf(":%u", quorumPaxosIDs.GetLength());
    FOREACH(it, quorumPaxosIDs)
    {
        buffer.Appendf(":%U:%U", it->quorumID, it->paxosID);
    }
    
    return true;
}

uint64_t QuorumPaxosID::GetPaxosID(List& quorumPaxosIDs, uint64_t quorumID)
{
    QuorumPaxosID*  it;

    FOREACH(it, quorumPaxosIDs)
    {
        if (it->quorumID == quorumID)
            return it->paxosID;
    }
    
    return 0;
}

ConfigShardServer::ConfigShardServer()
{
    prev = next = this;
    nextActivationTime = 0;
}

ConfigShardServer::ConfigShardServer(const ConfigShardServer& other)
{
    *this = other;
}

ConfigShardServer& ConfigShardServer::operator=(const ConfigShardServer& other)
{
    nodeID = other.nodeID;
    endpoint = other.endpoint;
    
    prev = next = this;
    
    return *this;
}

