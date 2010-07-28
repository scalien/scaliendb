#include "PaxosMessage.h"
#include "System/Common.h"

void PaxosMessage::Init(uint64_t paxosID_, char type_, uint64_t nodeID_)
{
	paxosID = paxosID_;
	nodeID = nodeID_;
	type = type_;
}

bool PaxosMessage::PrepareRequest(
 uint64_t paxosID_, uint64_t nodeID_,
 uint64_t proposalID_)
{
	Init(paxosID_, PAXOS_PREPARE_REQUEST, nodeID_);
	proposalID = proposalID_;

	return true;
}

bool PaxosMessage::PrepareRejected(
 uint64_t paxosID_, uint64_t nodeID_,
 uint64_t proposalID_, uint64_t promisedProposalID_)
{
	Init(paxosID_, PAXOS_PREPARE_REJECTED, nodeID_);
	proposalID = proposalID_;
	promisedProposalID = promisedProposalID_;
	
	return true;
}


bool PaxosMessage::PreparePreviouslyAccepted(
 uint64_t paxosID_, uint64_t nodeID_,
 uint64_t proposalID_, uint64_t acceptedProposalID_,
 uint64_t runID_, const Buffer& value_)
{
	Init(paxosID_, PAXOS_PREPARE_PREVIOUSLY_ACCEPTED, nodeID_);
	proposalID = proposalID_;
	acceptedProposalID = acceptedProposalID_;
	runID = runID_;
	value.Wrap(value_);
	
	return true;
}

bool PaxosMessage::PrepareCurrentlyOpen(
 uint64_t paxosID_, uint64_t nodeID_,
 uint64_t proposalID_)
{
	Init(paxosID_, PAXOS_PREPARE_CURRENTLY_OPEN, nodeID_);
	proposalID = proposalID_;

	return true;
}

bool PaxosMessage::ProposeRequest(
 uint64_t paxosID_, uint64_t nodeID_,
 uint64_t proposalID_, uint64_t runID_, const Buffer& value_)
{
	Init(paxosID_, PAXOS_PROPOSE_REQUEST, nodeID_);
	proposalID = proposalID_;
	runID = runID_;
	value.Wrap(value_);
	
	return true;
}

bool PaxosMessage::ProposeRejected(
 uint64_t paxosID_, uint64_t nodeID_,
 uint64_t proposalID_)
{
	Init(paxosID_, PAXOS_PROPOSE_REJECTED, nodeID_);
	proposalID = proposalID_;

	return true;
}

bool PaxosMessage::ProposeAccepted(
 uint64_t paxosID_, uint64_t nodeID_,
 uint64_t proposalID_)
{
	Init(paxosID_, PAXOS_PROPOSE_ACCEPTED, nodeID_);
	proposalID = proposalID_;

	return true;
}

bool PaxosMessage::LearnValue(
 uint64_t paxosID_, uint64_t nodeID_,
 uint64_t runID_, const Buffer& value_)
{
	Init(paxosID_, PAXOS_LEARN_VALUE, nodeID_);
	runID = runID_;
	value.Wrap(value_);
	
	return true;
}

bool PaxosMessage::LearnProposal(
 uint64_t paxosID_, uint64_t nodeID_,
 uint64_t proposalID_)
{
	Init(paxosID_, PAXOS_LEARN_PROPOSAL, nodeID_);
	proposalID = proposalID_;
	
	return true;
}

bool PaxosMessage::RequestChosen(
 uint64_t paxosID_, uint64_t nodeID_)
{
	Init(paxosID_, PAXOS_REQUEST_CHOSEN, nodeID_);
	
	return true;
}

//bool PaxosMessage::StartCatchup(
// uint64_t paxosID_, uint64_t nodeID_)
//{
//	Init(paxosID_, PAXOS_START_CATCHUP, nodeID_);
//	
//	return true;
//}

bool PaxosMessage::IsRequest() const
{
	return (type == PAXOS_PROPOSE_REQUEST ||
			type == PAXOS_PREPARE_REQUEST);
}

bool PaxosMessage::IsResponse() const
{
	return IsPrepareResponse() || IsProposeResponse();
}

bool PaxosMessage::IsPrepareResponse() const
{
	return (type == PAXOS_PREPARE_REJECTED ||
			type == PAXOS_PREPARE_PREVIOUSLY_ACCEPTED ||
			type == PAXOS_PREPARE_CURRENTLY_OPEN);
}

bool PaxosMessage::IsProposeResponse() const
{
	return (type == PAXOS_PROPOSE_REJECTED ||
			type == PAXOS_PROPOSE_ACCEPTED);
}

bool PaxosMessage::IsLearn() const
{
	return (type == PAXOS_LEARN_PROPOSAL ||
			type == PAXOS_LEARN_VALUE);
}

bool PaxosMessage::Read(const ReadBuffer& buffer)
{
	int		read;
	char	proto;
	
	if (buffer.GetLength() < 3)
		return false;

	switch (buffer.GetCharAt(2))
	{
		case PAXOS_PREPARE_REQUEST:
			read = buffer.Readf("%c:%c:%U:%U:%U", 
			 &proto, &type, &paxosID, &nodeID, &proposalID);
			break;
		case PAXOS_PREPARE_REJECTED:
			read = buffer.Readf("%c:%c:%U:%U:%U:%U",
			 &proto, &type, &paxosID, &nodeID, &proposalID, &promisedProposalID);
			break;
		case PAXOS_PREPARE_PREVIOUSLY_ACCEPTED:
			read = buffer.Readf("%c:%c:%U:%U:%U:%U:%U:%M",
			 &proto, &type, &paxosID, &nodeID, &proposalID, &acceptedProposalID, &runID, &value);
			break;
		case PAXOS_PREPARE_CURRENTLY_OPEN:
			read = buffer.Readf("%c:%c:%U:%U:%U",
			 &proto, &type, &paxosID, &nodeID, &proposalID);
			break;
		case PAXOS_PROPOSE_REQUEST:
			read = buffer.Readf("%c:%c:%U:%U:%U:%U:%M",
			 &proto, &type, &paxosID, &nodeID, &proposalID, &runID, &value);
			break;
		case PAXOS_PROPOSE_REJECTED:
			read = buffer.Readf("%c:%c:%U:%U:%U",
			 &proto, &type, &paxosID, &nodeID, &proposalID);
			break;
		case PAXOS_PROPOSE_ACCEPTED:
			read = buffer.Readf("%c:%c:%U:%U:%U",
			 &proto, &type, &paxosID, &nodeID, &proposalID);
			break;
		case PAXOS_LEARN_PROPOSAL:
			read = buffer.Readf("%c:%c:%U:%U:%U",
			 &proto, &type, &paxosID, &nodeID, &proposalID);
			break;
		case PAXOS_LEARN_VALUE:
			read = buffer.Readf("%c:%c:%U:%U:%U:%M",
			 &proto, &type, &paxosID, &nodeID, &runID, &value);
			break;
		case PAXOS_REQUEST_CHOSEN:
			read = buffer.Readf("%c:%c:%U:%U",
			 &proto, &type, &paxosID, &nodeID);
			break;
		case PAXOS_START_CATCHUP:
			read = buffer.Readf("%c:%c:%U:%U",
			 &proto, &type, &paxosID, &nodeID);
			break;
		default:
			return false;
	}
	
	assert(proto == PAXOS_PROTOCOL_ID);
	return (read == (signed)buffer.GetLength() ? true : false);

}

bool PaxosMessage::Write(Buffer& buffer) const
{
	const char proto = PAXOS_PROTOCOL_ID;
	
	switch (type)
	{
		case PAXOS_PREPARE_REQUEST:
			return buffer.Writef("%c:%c:%U:%U:%U",
			 proto, type, paxosID, nodeID, proposalID);
			break;
		case PAXOS_PREPARE_REJECTED:
			return buffer.Writef("%c:%c:%U:%U:%U:%U",
			 proto, type, paxosID, nodeID, proposalID, promisedProposalID);
			break;
		case PAXOS_PREPARE_PREVIOUSLY_ACCEPTED:
			return buffer.Writef("%c:%c:%U:%U:%U:%U:%U:%M",
			 proto, type, paxosID, nodeID, proposalID, acceptedProposalID, runID, &value);
			break;
		case PAXOS_PREPARE_CURRENTLY_OPEN:
			return buffer.Writef("%c:%c:%U:%U:%U",
			 proto, type, paxosID, nodeID, proposalID);
			break;
		case PAXOS_PROPOSE_REQUEST:
			return buffer.Writef("%c:%c:%U:%U:%U:%U:%M",
			 proto, type, paxosID, nodeID, proposalID, runID, &value);
			break;
		case PAXOS_PROPOSE_REJECTED:
			return buffer.Writef("%c:%c:%U:%U:%U",
			 proto, type, paxosID, nodeID, proposalID);
			break;
		case PAXOS_PROPOSE_ACCEPTED:
			return buffer.Writef("%c:%c:%U:%U:%U",
			 proto, type, paxosID, nodeID, proposalID);
			break;
		case PAXOS_LEARN_PROPOSAL:
			return buffer.Writef("%c:%c:%U:%U:%U",
			 proto, type, paxosID, nodeID, proposalID);
			break;
		case PAXOS_LEARN_VALUE:
			return buffer.Writef("%c:%c:%U:%U:%U:%M",
			 proto, type, paxosID, nodeID, runID, &value);
			break;
		case PAXOS_REQUEST_CHOSEN:
			return buffer.Writef("%c:%c:%U:%U",
			 proto, type, paxosID, nodeID);
			break;
		case PAXOS_START_CATCHUP:
			return buffer.Writef("%c:%c:%U:%U",
			 proto, type, paxosID, nodeID);
			break;
		default:
			return false;
	}
}
