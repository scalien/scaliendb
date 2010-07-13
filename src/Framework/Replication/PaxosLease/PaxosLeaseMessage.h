#ifndef PAXOSLEASEMESSAGE_H
#define PAXOSLEASEMESSAGE_H

#include "Framework/Replication/ReplicationMessage.h"

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

class PaxosLeaseMessage : public ReplicationMessage
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
	
	void			Init(char type, unsigned nodeID);
		
	bool			PrepareRequest(unsigned nodeID, uint64_t proposalID, uint64_t paxosID);
	bool			PrepareRejected(unsigned nodeID, uint64_t proposalID);
	bool			PreparePreviouslyAccepted(unsigned nodeID, uint64_t proposalID,
					 uint64_t acceptedProposalID, unsigned leaseOwner, unsigned duration);
	bool			PrepareCurrentlyOpen(unsigned nodeID, uint64_t proposalID);	
	bool			ProposeRequest(unsigned nodeID, uint64_t proposalID,
					 unsigned leaseOwner, unsigned duration);
	bool			ProposeRejected(unsigned nodeID, uint64_t proposalID);
	bool			ProposeAccepted(unsigned nodeID, uint64_t proposalID);
	bool			LearnChosen(unsigned nodeID, unsigned leaseOwner, unsigned duration,
					 uint64_t localExpireTime);
	
	bool			IsRequest() const;
	bool			IsPrepareResponse() const;
	bool			IsProposeResponse() const;
	bool			IsResponse() const;
	bool			IsLearnChosen() const;	

	// implementation of ReplicationMessage interface:
	bool			Read(const Buffer* buffer);
	bool			Write(Buffer* buffer) const;
};

#endif
