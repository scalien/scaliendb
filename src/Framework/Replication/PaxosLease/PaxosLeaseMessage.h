#ifndef PAXOSLEASEMESSAGE_H
#define PAXOSLEASEMESSAGE_H

#include "Framework/Messaging/Message.h"

#define PAXOSLEASE_PREPARE_REQUEST				'1'
#define PAXOSLEASE_PREPARE_REJECTED				'2'
#define PAXOSLEASE_PREPARE_PREVIOUSLY_ACCEPTED	'3'
#define PAXOSLEASE_PREPARE_CURRENTLY_OPEN		'4'
#define PAXOSLEASE_PROPOSE_REQUEST				'5'
#define PAXOSLEASE_PROPOSE_REJECTED				'6'
#define PAXOSLEASE_PROPOSE_ACCEPTED				'7'
#define PAXOSLEASE_LEARN_CHOSEN					'8'

/*
===============================================================================

 PaxosLeaseMessage

===============================================================================
*/

class PaxosLeaseMessage : public Message
{
public:
	char			type;
	unsigned		nodeID;
	uint64_t		proposalID;
	
	uint64_t		acceptedProposalID;
	unsigned		leaseOwner;
	unsigned		duration;
	uint64_t		localExpireTime;
	uint64_t		paxosID; // so only up-to-date nodes can become masters
	
	void			Init(char type, uint64_t nodeID);
		
	bool			PrepareRequest(uint64_t nodeID, uint64_t proposalID, uint64_t paxosID);
	bool			PrepareRejected(uint64_t nodeID, uint64_t proposalID);
	bool			PreparePreviouslyAccepted(uint64_t nodeID, uint64_t proposalID,
					 uint64_t acceptedProposalID, unsigned leaseOwner, unsigned duration);
	bool			PrepareCurrentlyOpen(uint64_t nodeID, uint64_t proposalID);	
	bool			ProposeRequest(uint64_t nodeID, uint64_t proposalID,
					 unsigned leaseOwner, unsigned duration);
	bool			ProposeRejected(uint64_t nodeID, uint64_t proposalID);
	bool			ProposeAccepted(uint64_t nodeID, uint64_t proposalID);
	bool			LearnChosen(uint64_t nodeID, unsigned leaseOwner, unsigned duration,
					 uint64_t localExpireTime);
	
	bool			IsRequest() const;
	bool			IsPrepareResponse() const;
	bool			IsProposeResponse() const;
	bool			IsResponse() const;
	bool			IsLearnChosen() const;	

	// implementation of Message interface:
	bool			Read(const ReadBuffer& buffer);
	bool			Write(Buffer& buffer) const;
};

#endif
