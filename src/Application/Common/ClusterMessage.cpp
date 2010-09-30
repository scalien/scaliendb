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
    if (configState != NULL)
        delete configState;
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
    int         read;
        
    if (buffer.GetLength() < 1)
        return false;
    
    switch (buffer.GetCharAt(0))
    {
        case CLUSTERMESSAGE_SET_NODEID:
            read = buffer.Readf("%c:%U",
             &type, &nodeID);
            break;
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            if (configState)
                delete configState;
            configState = new ConfigState;
            return configState->Read(buffer, true);
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
    switch (type)
    {
        case CLUSTERMESSAGE_SET_NODEID:
            buffer.Writef("%c:%U",
             type, nodeID);
            return true;
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            return configState->Write(buffer, true);
            return true;
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