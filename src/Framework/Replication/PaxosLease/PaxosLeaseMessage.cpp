#include "PaxosLeaseMessage.h"

void PaxosLeaseMessage::Init(char type_, unsigned nodeID_)
{
	type = type_;
	nodeID = nodeID_;
}

bool PaxosLeaseMessage::PrepareRequest(unsigned nodeID_, uint64_t proposalID_, uint64_t paxosID_)
{
	Init(PAXOSLEASE_PREPARE_REQUEST, nodeID_);
	proposalID = proposalID_;
	paxosID = paxosID_;
	
	return true;
}

bool PaxosLeaseMessage::PrepareRejected(unsigned nodeID_, uint64_t proposalID_)
{
	Init(PAXOSLEASE_PREPARE_REJECTED, nodeID_);
	proposalID = proposalID_;

	return true;
}

bool PaxosLeaseMessage::PreparePreviouslyAccepted(unsigned nodeID_, uint64_t proposalID_,
 uint64_t acceptedProposalID_, unsigned leaseOwner_, unsigned duration_)
{
	Init(PAXOSLEASE_PREPARE_PREVIOUSLY_ACCEPTED, nodeID_);
	proposalID = proposalID_;
	acceptedProposalID = acceptedProposalID_;
	leaseOwner = leaseOwner_;
	duration = duration_;
	
	return true;
}

bool PaxosLeaseMessage::PrepareCurrentlyOpen(unsigned nodeID_, uint64_t proposalID_)
{
	Init(PAXOSLEASE_PREPARE_CURRENTLY_OPEN, nodeID_);
	proposalID = proposalID_;

	return true;
}

bool PaxosLeaseMessage::ProposeRequest(unsigned nodeID_,
 uint64_t proposalID_, unsigned int leaseOwner_, unsigned duration_)
{
	Init(PAXOSLEASE_PROPOSE_REQUEST, nodeID_);
	proposalID = proposalID_;
	leaseOwner = leaseOwner_;
	duration = duration_;
	
	return true;
}

bool PaxosLeaseMessage::ProposeRejected(unsigned nodeID_, uint64_t proposalID_)
{
	Init(PAXOSLEASE_PROPOSE_REJECTED, nodeID_);
	proposalID = proposalID_;

	return true;
}

bool PaxosLeaseMessage::ProposeAccepted(unsigned nodeID_, uint64_t proposalID_)
{
	Init(PAXOSLEASE_PROPOSE_ACCEPTED, nodeID_);
	proposalID = proposalID_;

	return true;
}

bool PaxosLeaseMessage::LearnChosen(unsigned nodeID_,
 unsigned leaseOwner_, unsigned duration_, uint64_t localExpireTime_)
{
	Init(PAXOSLEASE_LEARN_CHOSEN, nodeID_);
	leaseOwner = leaseOwner_;
	duration = duration_;
	localExpireTime = localExpireTime_;

	return true;
}

bool PaxosLeaseMessage::IsRequest() const
{
	return (type == PAXOSLEASE_PROPOSE_REQUEST || type == PAXOSLEASE_PREPARE_REQUEST);
}

bool PaxosLeaseMessage::IsResponse() const
{
	return IsPrepareResponse() || IsProposeResponse();
}

bool PaxosLeaseMessage::IsPrepareResponse() const
{
	return (type == PAXOSLEASE_PREPARE_REJECTED ||
			type == PAXOSLEASE_PREPARE_PREVIOUSLY_ACCEPTED ||
			type == PAXOSLEASE_PREPARE_CURRENTLY_OPEN);
}

bool PaxosLeaseMessage::IsProposeResponse() const
{
	return (type == PAXOSLEASE_PROPOSE_REJECTED || type == PAXOSLEASE_PROPOSE_ACCEPTED);
}

bool PaxosLeaseMessage::IsLearnChosen() const
{
	return (type == PAXOSLEASE_LEARN_CHOSEN);
}

bool PaxosLeaseMessage::Read(const Buffer* buffer)
{
	int read;
	
	if (buffer->GetLength() < 1)
		return false;
	
	switch (buffer->GetCharAt(0))
	{
		case PAXOSLEASE_PREPARE_REQUEST:
			read = buffer->Readf("%c:%u:%U:%U", &type, &nodeID, &proposalID, &paxosID);
			break;
		case PAXOSLEASE_PREPARE_REJECTED:
			read = buffer->Readf("%c:%u:%U", &type, &nodeID, &proposalID);
			break;
		case PAXOSLEASE_PREPARE_PREVIOUSLY_ACCEPTED:
			read = buffer->Readf("%c:%u:%U:%U:%u:%u",
			 &type, &nodeID, &proposalID, &acceptedProposalID, &leaseOwner, &duration);
			break;
		case PAXOSLEASE_PREPARE_CURRENTLY_OPEN:
			read = buffer->Readf("%c:%u:%U", &type, &nodeID, &proposalID);
			break;
		case PAXOSLEASE_PROPOSE_REQUEST:
			read = buffer->Readf("%c:%u:%U:%u:%u", &type, &nodeID, &proposalID,
			 &leaseOwner, &duration);
			break;
		case PAXOSLEASE_PROPOSE_REJECTED:
			read = buffer->Readf("%c:%u:%U", &type, &nodeID, &proposalID);
			break;
		case PAXOSLEASE_PROPOSE_ACCEPTED:
			read = buffer->Readf("%c:%u:%U", &type, &nodeID, &proposalID);
			break;
		case PAXOSLEASE_LEARN_CHOSEN:
			read = buffer->Readf("%c:%u:%u:%u:%U", &type, &nodeID, &leaseOwner,
			 &duration, &localExpireTime);
			break;
		default:
			return false;
	}

	return (read == (signed)buffer->GetLength() ? true : false);
}

bool PaxosLeaseMessage::Write(Buffer* buffer) const
{
	switch (type)
	{
		case PAXOSLEASE_PREPARE_REQUEST:
			return buffer->Writef("%c:%u:%U:%U", type, nodeID, proposalID, paxosID);
			break;
		case PAXOSLEASE_PREPARE_REJECTED:
			return buffer->Writef("%c:%u:%U", type, nodeID, proposalID);
			break;
		case PAXOSLEASE_PREPARE_PREVIOUSLY_ACCEPTED:
			return buffer->Writef("%c:%u:%U:%U:%u:%u",
			 type, nodeID, proposalID, acceptedProposalID, leaseOwner, duration);
			break;
		case PAXOSLEASE_PREPARE_CURRENTLY_OPEN:
			return buffer->Writef("%c:%u:%U", type, nodeID, proposalID);
			break;
		case PAXOSLEASE_PROPOSE_REQUEST:
			return buffer->Writef("%c:%u:%U:%u:%u", type, nodeID, proposalID, leaseOwner, duration);
			break;
		case PAXOSLEASE_PROPOSE_REJECTED:
			return buffer->Writef("%c:%u:%U", type, nodeID, proposalID);
			break;
		case PAXOSLEASE_PROPOSE_ACCEPTED:
			return buffer->Writef("%c:%u:%U", type, nodeID, proposalID);
			break;
		case PAXOSLEASE_LEARN_CHOSEN:
			return buffer->Writef("%c:%u:%u:%u:%U",
			 type, nodeID, leaseOwner, duration, localExpireTime);
			break;
		default:
			return false;
	}
}
