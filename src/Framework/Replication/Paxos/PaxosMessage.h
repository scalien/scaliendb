#ifndef PAXOSMESSAGE_H
#define PAXOSMESSAGE_H

#include "Framework/Messaging/Message.h"

#define PAXOS_PREPARE_REQUEST				'1'
#define PAXOS_PREPARE_REJECTED				'2'
#define PAXOS_PREPARE_PREVIOUSLY_ACCEPTED	'3'
#define PAXOS_PREPARE_CURRENTLY_OPEN		'4'
#define PAXOS_PROPOSE_REQUEST				'5'
#define PAXOS_PROPOSE_REJECTED				'6'
#define PAXOS_PROPOSE_ACCEPTED				'7'
#define PAXOS_LEARN_PROPOSAL				'8'
#define PAXOS_LEARN_VALUE					'9'
#define PAXOS_REQUEST_CHOSEN				'0'
#define PAXOS_START_CATCHUP					'c'

/*
===============================================================================

 PaxosMessage

===============================================================================
*/

class PaxosMessage : public Message
{
public:
	uint64_t		paxosID;
	char			type;
	uint64_t		nodeID;
	uint64_t		runID;
	uint64_t		proposalID;
	uint64_t		acceptedProposalID;
	uint64_t		promisedProposalID;
	Buffer*			value;

	void			Init(uint64_t paxosID, char type, uint64_t nodeID);
		
	bool			PrepareRequest(
					 uint64_t paxosID, uint64_t nodeID,
					 uint64_t proposalID);

	bool			PrepareRejected(
					 uint64_t paxosID, uint64_t nodeID,
					 uint64_t proposalID, uint64_t promisedProposalID);

	bool			PreparePreviouslyAccepted(
					 uint64_t paxosID, uint64_t nodeID,
					 uint64_t proposalID, uint64_t acceptedProposalID,
					 uint64_t runID, Buffer* value);

	bool			PrepareCurrentlyOpen(
					 uint64_t paxosID, uint64_t nodeID,
					 uint64_t proposalID);

	bool			ProposeRequest(
					 uint64_t paxosID, uint64_t nodeID,
					 uint64_t proposalID, uint64_t runID, Buffer* value);

	bool			ProposeRejected(
					 uint64_t paxosID, uint64_t nodeID,
					 uint64_t proposalID);

	bool			ProposeAccepted(
					 uint64_t paxosID, uint64_t nodeID,
					 uint64_t proposalID);

	bool			LearnProposal(
					 uint64_t paxosID, uint64_t nodeID,
					 uint64_t proposalID);

	bool			LearnValue(
					 uint64_t paxosID, uint64_t nodeID,
					 uint64_t runID, Buffer* value);

	bool			RequestChosen(
					 uint64_t paxosID, uint64_t nodeID);

//	bool			StartCatchup(uint64_t paxosID, uint64_t nodeID);

	bool			IsRequest() const;
	bool			IsPrepareResponse() const;
	bool			IsProposeResponse() const;
	bool			IsResponse() const;
	bool			IsLearn() const;

	// implementation of Message interface:
	bool			Read(const ReadBuffer& buffer);
	bool			Write(Buffer& buffer) const;
};

#endif
