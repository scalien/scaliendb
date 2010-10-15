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

bool ClusterMessage::Heartbeat(uint64_t nodeID_, QuorumPaxosIDList& quorumPaxosIDs_)
{
    type = CLUSTERMESSAGE_HEARTBEAT;
    nodeID = nodeID_;
    quorumPaxosIDs = quorumPaxosIDs_;
    return true;
}

bool ClusterMessage::SetConfigState(ConfigState& configState_)
{
    type = CLUSTERMESSAGE_SET_CONFIG_STATE;
    configState = configState_;
    return true;
}

bool ClusterMessage::RequestLease(uint64_t nodeID_, uint64_t quorumID_,
 uint64_t proposalID_, unsigned duration_)
{
    type = CLUSTERMESSAGE_REQUEST_LEASE;
    nodeID = nodeID_;
    quorumID = quorumID_;
    proposalID = proposalID_;
    duration = duration_;
    return true;
}

bool ClusterMessage::ReceiveLease(uint64_t nodeID_, uint64_t quorumID_,
 uint64_t proposalID_, unsigned duration_)
{
    type = CLUSTERMESSAGE_RECEIVE_LEASE;
    nodeID = nodeID_;
    quorumID = quorumID_;
    proposalID = proposalID_;
    duration = duration_;
    return true;
}

bool ClusterMessage::Read(ReadBuffer& buffer)
{
    int             read;
    ReadBuffer      tempBuffer;
    unsigned        i, length;
        
    if (buffer.GetLength() < 1)
        return false;
    
    switch (buffer.GetCharAt(0))
    {
        case CLUSTERMESSAGE_SET_NODEID:
            read = buffer.Readf("%c:%U",
             &type, &nodeID);
            break;
        case CLUSTERMESSAGE_HEARTBEAT:
            read = buffer.Readf("%c:%U",
             &type, &nodeID);
            if (read < 3)
                return false;
            buffer.Advance(read);
            read = buffer.Readf(":%U", &length);
            if (read < 2)
                return false;
            buffer.Advance(read);
            for (i = 0; i < length; i++)
            {
                read = buffer.Readf(":%U:%U");
                if (read < 4)
                    return false;
                buffer.Advance(read);
            }
            return true;
            break;
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            type = CLUSTERMESSAGE_SET_CONFIG_STATE;
            return configState.Read(buffer, true);
        case CLUSTERMESSAGE_REQUEST_LEASE:
            read = buffer.Readf("%c:%U:%U:%U:%u",
             &type, &nodeID, &quorumID, &proposalID, &duration);
            break;
        case CLUSTERMESSAGE_RECEIVE_LEASE:
            read = buffer.Readf("%c:%U:%U:%U:%u",
             &type, &nodeID, &quorumID, &proposalID, &duration);
            break;
        default:
            return false;
    }
    
    return (read == (signed)buffer.GetLength());
}

bool ClusterMessage::Write(Buffer& buffer)
{
    QuorumPaxosID*  it;
    Buffer          tempBuffer;
    
    switch (type)
    {
        case CLUSTERMESSAGE_SET_NODEID:
            buffer.Writef("%c:%U",
             type, nodeID);
            return true;
        case CLUSTERMESSAGE_HEARTBEAT:
            buffer.Writef("%c:%U",
             type, nodeID);
            buffer.Appendf(":%U", quorumPaxosIDs.GetLength());
            FOREACH(it, quorumPaxosIDs)
            {
                buffer.Writef(":%U:%U", it->quorumID, it->paxosID);
            }
            return true;
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            buffer.Clear();
            return configState.Write(buffer, true);
        case CLUSTERMESSAGE_REQUEST_LEASE:
            buffer.Writef("%c:%U:%U:%U:%u",
             type, nodeID, quorumID, proposalID, duration);
            return true;
        case CLUSTERMESSAGE_RECEIVE_LEASE:
            buffer.Writef("%c:%U:%U:%U:%u",
             type, nodeID, quorumID, proposalID, duration);
            return true;
        default:
            return false;
    }
    
    return true;
}