#include "ClusterMessage.h"

ClusterMessage::ClusterMessage()
{
    configState = NULL;
}

bool ClusterMessage::SetNodeID(uint64_t nodeID_)
{
    type = CLUSTERMESSAGE_SET_NODEID;
    nodeID = nodeID_;
    return true;
}

bool ClusterMessage::SetConfigState(ConfigState* configState_)
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

bool ClusterMessage::ConfigCatchup(uint64_t paxosID_, ConfigState* configState_)
{
    type = CLUSTERMESSAGE_CONFIG_CATCHUP;
    paxosID = paxosID_;
    configState = configState_;
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
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            type = CLUSTERMESSAGE_SET_CONFIG_STATE;
            assert(configState != NULL);
            return configState->Read(buffer, true);
        case CLUSTERMESSAGE_REQUEST_LEASE:
            read = buffer.Readf("%c:%U:%U:%U:%u",
             &type, &nodeID, &quorumID, &proposalID, &duration);
            break;
        case CLUSTERMESSAGE_RECEIVE_LEASE:
            read = buffer.Readf("%c:%U:%U:%U:%u",
             &type, &nodeID, &quorumID, &proposalID, &duration);
            break;
        case CLUSTERMESSAGE_CONFIG_CATCHUP:
            tempBuffer = buffer;
            read = tempBuffer.Readf("%c:%U:", &type, &paxosID);
            if (read < 4)
                return false;
            tempBuffer.Advance(read);
            return configState->Read(tempBuffer);
        default:
            return false;
    }
    
    return (read == (signed)buffer.GetLength());
}

bool ClusterMessage::Write(Buffer& buffer)
{
    Buffer tempBuffer;
    
    switch (type)
    {
        case CLUSTERMESSAGE_SET_NODEID:
            buffer.Writef("%c:%U",
             type, nodeID);
            return true;
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            return configState->Write(buffer, true);
        case CLUSTERMESSAGE_REQUEST_LEASE:
            buffer.Writef("%c:%U:%U:%U:%u",
             type, nodeID, quorumID, proposalID, duration);
            return true;
        case CLUSTERMESSAGE_RECEIVE_LEASE:
            buffer.Writef("%c:%U:%U:%U:%u",
             type, nodeID, quorumID, proposalID, duration);
            return true;
        case CLUSTERMESSAGE_CONFIG_CATCHUP:
            if (!configState->Write(tempBuffer))
                return false;
            buffer.Writef("%c:%U:%B",
             type, paxosID, &tempBuffer);
            return true;
        default:
            return false;
    }
    
    return true;
}