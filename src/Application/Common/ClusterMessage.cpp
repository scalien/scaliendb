#include "ClusterMessage.h"

ClusterMessage::ClusterMessage()
{
}

bool ClusterMessage::SetNodeID(uint64_t nodeID_)
{
    type = CLUSTERMESSAGE_SET_NODEID;
    nodeID = nodeID_;
    return true;
}

bool ClusterMessage::Heartbeat(uint64_t nodeID_,
 List<QuorumPaxosID>& quorumPaxosIDs_, List<QuorumShardInfo>& quorumShardInfos_,
 unsigned httpPort_, unsigned sdbpPort_)
{
    type = CLUSTERMESSAGE_HEARTBEAT;
    nodeID = nodeID_;
    quorumPaxosIDs = quorumPaxosIDs_;
    quorumShardInfos = quorumShardInfos_;
    httpPort = httpPort_;
    sdbpPort = sdbpPort_;
    return true;
}

bool ClusterMessage::SetConfigState(ConfigState& configState_)
{
    type = CLUSTERMESSAGE_SET_CONFIG_STATE;
    configState = configState_;
    return true;
}

bool ClusterMessage::RequestLease(uint64_t nodeID_, uint64_t quorumID_,
 uint64_t proposalID_, uint64_t paxosID_, uint64_t configID_, unsigned duration_)
{
    type = CLUSTERMESSAGE_REQUEST_LEASE;
    nodeID = nodeID_;
    quorumID = quorumID_;
    proposalID = proposalID_;
    paxosID = paxosID_;
    configID = configID_;
    duration = duration_;
    return true;
}

bool ClusterMessage::ReceiveLease(uint64_t nodeID_, uint64_t quorumID_,
 uint64_t proposalID_, uint64_t configID_, unsigned duration_,
 bool watchingPaxosID_, List<uint64_t>& activeNodes_)
{
    type = CLUSTERMESSAGE_RECEIVE_LEASE;
    nodeID = nodeID_;
    quorumID = quorumID_;
    proposalID = proposalID_;
    configID = configID_;
    duration = duration_;
    activeNodes = activeNodes_;
    watchingPaxosID = watchingPaxosID_;
    return true;
}

bool ClusterMessage::Read(ReadBuffer& buffer)
{
    int             read;
    ReadBuffer      tempBuffer;
        
    if (buffer.GetLength() < 1)
        return false;
    
    switch (buffer.GetCharAt(0))
    {
        case CLUSTERMESSAGE_SET_NODEID:
            read = buffer.Readf("%c:%U",
             &type, &nodeID);
            break;
        case CLUSTERMESSAGE_HEARTBEAT:
            read = buffer.Readf("%c:%U:%u:%u",
             &type, &nodeID, &httpPort, &sdbpPort);
            if (read < 3)
                return false;
            buffer.Advance(read);
            if (!QuorumPaxosID::ReadList(buffer, quorumPaxosIDs))
                return false;
            if (!QuorumShardInfo::ReadList(buffer, quorumShardInfos))
                return false;
            return true;
            break;
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            type = CLUSTERMESSAGE_SET_CONFIG_STATE;
            return configState.Read(buffer, true);
        case CLUSTERMESSAGE_REQUEST_LEASE:
            read = buffer.Readf("%c:%U:%U:%U:%U:%U:%u",
             &type, &nodeID, &quorumID, &proposalID, &paxosID, &configID, &duration);
            break;
        case CLUSTERMESSAGE_RECEIVE_LEASE:
            read = buffer.Readf("%c:%U:%U:%U:%U:%u:%b",
             &type, &nodeID, &quorumID, &proposalID, &configID, &duration, &watchingPaxosID);
             if (read < 9)
                return false;
            buffer.Advance(read);
            read = buffer.Readf(":");
            if (read != 1)
                return false;
            buffer.Advance(read);
            return ConfigState::ReadIDList< List<uint64_t> >(activeNodes, buffer);
        default:
            return false;
    }
    
    return (read == (signed)buffer.GetLength());
}

bool ClusterMessage::Write(Buffer& buffer)
{
    Buffer          tempBuffer;
    
    switch (type)
    {
        case CLUSTERMESSAGE_SET_NODEID:
            buffer.Writef("%c:%U",
             type, nodeID);
            return true;
        case CLUSTERMESSAGE_HEARTBEAT:
            buffer.Writef("%c:%U:%u:%u",
             type, nodeID, httpPort, sdbpPort);
            QuorumPaxosID::WriteList(buffer, quorumPaxosIDs);
            QuorumShardInfo::WriteList(buffer, quorumShardInfos);
            return true;
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            buffer.Clear();
            return configState.Write(buffer, true);
        case CLUSTERMESSAGE_REQUEST_LEASE:
            buffer.Writef("%c:%U:%U:%U:%U:%U:%u",
             type, nodeID, quorumID, proposalID, paxosID, configID, duration);
            return true;
        case CLUSTERMESSAGE_RECEIVE_LEASE:
            buffer.Writef("%c:%U:%U:%U:%U:%u:%b",
             type, nodeID, quorumID, proposalID, configID, duration, watchingPaxosID);
            buffer.Appendf(":");
            ConfigState::WriteIDList< List<uint64_t> >(activeNodes, buffer);
            return true;
        default:
            return false;
    }
}