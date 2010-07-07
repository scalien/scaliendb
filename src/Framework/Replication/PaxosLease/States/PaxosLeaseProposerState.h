#ifndef PAXOSLEASEPROPOSERSTATE_H
#define PAXOSLEASEPROPOSERSTATE_H

#include "System/Platform.h"

/*
===============================================================================

 PaxosLeaseProposerState

===============================================================================
*/

struct PaxosLeaseProposerState
{
	bool Active()
	{
		return (preparing || proposing);
	}
	
	void Init()
	{
		preparing =	false;
		proposing =	false;
		proposalID = 0;
		highestReceivedProposalID =	0;
		duration = 0;
		expireTime = 0;
	}
	
	bool		preparing;
	bool		proposing;
	uint64_t	proposalID;
	uint64_t	highestReceivedProposalID;
	unsigned	leaseOwner;
	unsigned	duration;
	uint64_t	expireTime;

};

#endif
