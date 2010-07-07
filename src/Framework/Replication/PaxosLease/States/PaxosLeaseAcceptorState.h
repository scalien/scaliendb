#ifndef PAXOSLEASEACCEPTORSTATE_H
#define PAXOSLEASEACCEPTORSTATE_H

#include "System/Platform.h"

/*
===============================================================================

 PaxosLeaseAcceptorState

===============================================================================
*/

struct PaxosLeaseAcceptorState
{
	void Init()
	{
		promisedProposalID = 0;

		accepted = false;
		acceptedProposalID	= 0;
		acceptedLeaseOwner	= 0;
		acceptedDuration	= 0;
		acceptedExpireTime	= 0;
	}

	void OnLeaseTimeout()
	{
		accepted = false;
		acceptedProposalID = 0;
		acceptedLeaseOwner = 0;
		acceptedDuration   = 0;
		acceptedExpireTime = 0;
	}

	uint64_t	promisedProposalID;
	bool		accepted;
	uint64_t	acceptedProposalID;
	unsigned	acceptedLeaseOwner;
	unsigned	acceptedDuration;
	uint64_t	acceptedExpireTime;
};

#endif
