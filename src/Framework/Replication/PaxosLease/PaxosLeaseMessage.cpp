#include "PaxosLeaseMessage.h"

void PaxosLeaseMessage::Init(char type_, uint64_t nodeID_)
{
    type = type_;
    nodeID = nodeID_;
}

bool PaxosLeaseMessage::PrepareRequest(uint64_t nodeID_, uint64_t proposalID_, uint64_t paxosID_)
{
    Init(PAXOSLEASE_PREPARE_REQUEST, nodeID_);
    proposalID = proposalID_;
    paxosID = paxosID_;
    
    return true;
}

bool PaxosLeaseMessage::PrepareRejected(uint64_t nodeID_, uint64_t proposalID_)
{
    Init(PAXOSLEASE_PREPARE_REJECTED, nodeID_);
    proposalID = proposalID_;

    return true;
}

bool PaxosLeaseMessage::PreparePreviouslyAccepted(uint64_t nodeID_, uint64_t proposalID_,
 uint64_t acceptedProposalID_, uint64_t leaseOwner_, unsigned duration_)
{
    Init(PAXOSLEASE_PREPARE_PREVIOUSLY_ACCEPTED, nodeID_);
    proposalID = proposalID_;
    acceptedProposalID = acceptedProposalID_;
    leaseOwner = leaseOwner_;
    duration = duration_;
    
    return true;
}

bool PaxosLeaseMessage::PrepareCurrentlyOpen(uint64_t nodeID_, uint64_t proposalID_)
{
    Init(PAXOSLEASE_PREPARE_CURRENTLY_OPEN, nodeID_);
    proposalID = proposalID_;

    return true;
}

bool PaxosLeaseMessage::ProposeRequest(uint64_t nodeID_,
 uint64_t proposalID_, uint64_t leaseOwner_, unsigned duration_)
{
    Init(PAXOSLEASE_PROPOSE_REQUEST, nodeID_);
    proposalID = proposalID_;
    leaseOwner = leaseOwner_;
    duration = duration_;
    
    return true;
}

bool PaxosLeaseMessage::ProposeRejected(uint64_t nodeID_, uint64_t proposalID_)
{
    Init(PAXOSLEASE_PROPOSE_REJECTED, nodeID_);
    proposalID = proposalID_;

    return true;
}

bool PaxosLeaseMessage::ProposeAccepted(uint64_t nodeID_, uint64_t proposalID_)
{
    Init(PAXOSLEASE_PROPOSE_ACCEPTED, nodeID_);
    proposalID = proposalID_;

    return true;
}

bool PaxosLeaseMessage::LearnChosen(uint64_t nodeID_, uint64_t leaseOwner_,
 unsigned duration_, uint64_t localExpireTime_, uint64_t paxosID_)
{
    Init(PAXOSLEASE_LEARN_CHOSEN, nodeID_);
    leaseOwner = leaseOwner_;
    duration = duration_;
    localExpireTime = localExpireTime_;
    paxosID = paxosID_;

    return true;
}

bool PaxosLeaseMessage::IsRequest()
{
    return (type == PAXOSLEASE_PROPOSE_REQUEST || type == PAXOSLEASE_PREPARE_REQUEST);
}

bool PaxosLeaseMessage::IsResponse()
{
    return IsPrepareResponse() || IsProposeResponse();
}

bool PaxosLeaseMessage::IsPrepareResponse()
{
    return (type == PAXOSLEASE_PREPARE_REJECTED ||
            type == PAXOSLEASE_PREPARE_PREVIOUSLY_ACCEPTED ||
            type == PAXOSLEASE_PREPARE_CURRENTLY_OPEN);
}

bool PaxosLeaseMessage::IsProposeResponse()
{
    return (type == PAXOSLEASE_PROPOSE_REJECTED || type == PAXOSLEASE_PROPOSE_ACCEPTED);
}

bool PaxosLeaseMessage::IsLearnChosen()
{
    return (type == PAXOSLEASE_LEARN_CHOSEN);
}

bool PaxosLeaseMessage::Read(ReadBuffer& buffer)
{
    int     read;
    char    proto;
    
    if (buffer.GetLength() < 3)
        return false;
    
    switch (buffer.GetCharAt(2))
    {
        case PAXOSLEASE_PREPARE_REQUEST:
            read = buffer.Readf("%c:%c:%U:%U:%U",
             &proto, &type, &nodeID, &proposalID, &paxosID);
            break;
        case PAXOSLEASE_PREPARE_REJECTED:
            read = buffer.Readf("%c:%c:%U:%U",
             &proto, &type, &nodeID, &proposalID);
            break;
        case PAXOSLEASE_PREPARE_PREVIOUSLY_ACCEPTED:
            read = buffer.Readf("%c:%c:%U:%U:%U:%U:%u",
             &proto, &type, &nodeID, &proposalID, &acceptedProposalID, &leaseOwner, &duration);
            break;
        case PAXOSLEASE_PREPARE_CURRENTLY_OPEN:
            read = buffer.Readf("%c:%c:%U:%U",
             &proto, &type, &nodeID, &proposalID);
            break;
        case PAXOSLEASE_PROPOSE_REQUEST:
            read = buffer.Readf("%c:%c:%U:%U:%U:%u",
             &proto, &type, &nodeID, &proposalID, &leaseOwner, &duration);
            break;
        case PAXOSLEASE_PROPOSE_REJECTED:
            read = buffer.Readf("%c:%c:%U:%U",
             &proto, &type, &nodeID, &proposalID);
            break;
        case PAXOSLEASE_PROPOSE_ACCEPTED:
            read = buffer.Readf("%c:%c:%U:%U",
             &proto, &type, &nodeID, &proposalID);
            break;
        case PAXOSLEASE_LEARN_CHOSEN:
            read = buffer.Readf("%c:%c:%U:%U:%u:%U:%U",
             &proto, &type, &nodeID, &leaseOwner, &duration, &localExpireTime, &paxosID);
            break;
        default:
            return false;
    }

    assert(proto == PAXOSLEASE_PROTOCOL_ID);
    return (read == (signed)buffer.GetLength() ? true : false);
}

bool PaxosLeaseMessage::Write(Buffer& buffer)
{
    const char proto = PAXOSLEASE_PROTOCOL_ID;
    
    switch (type)
    {
        case PAXOSLEASE_PREPARE_REQUEST:
            buffer.Writef("%c:%c:%U:%U:%U",
             proto, type, nodeID, proposalID, paxosID);
            break;
        case PAXOSLEASE_PREPARE_REJECTED:
            buffer.Writef("%c:%c:%U:%U",
             proto, type, nodeID, proposalID);
            break;
        case PAXOSLEASE_PREPARE_PREVIOUSLY_ACCEPTED:
            buffer.Writef("%c:%c:%U:%U:%U:%U:%u",
             proto, type, nodeID, proposalID, acceptedProposalID, leaseOwner, duration);
            break;
        case PAXOSLEASE_PREPARE_CURRENTLY_OPEN:
            buffer.Writef("%c:%c:%U:%U",
             proto, type, nodeID, proposalID);
            break;
        case PAXOSLEASE_PROPOSE_REQUEST:
            buffer.Writef("%c:%c:%U:%U:%U:%u",
             proto, type, nodeID, proposalID, leaseOwner, duration);
            break;
        case PAXOSLEASE_PROPOSE_REJECTED:
            buffer.Writef("%c:%c:%U:%U",
             proto, type, nodeID, proposalID);
            break;
        case PAXOSLEASE_PROPOSE_ACCEPTED:
            buffer.Writef("%c:%c:%U:%U",
             proto, type, nodeID, proposalID);
            break;
        case PAXOSLEASE_LEARN_CHOSEN:
            buffer.Writef("%c:%c:%U:%U:%u:%U:%U",
             proto, type, nodeID, leaseOwner, duration, localExpireTime, paxosID);
            break;
        default:
            return false;
    }

	return true;
}
