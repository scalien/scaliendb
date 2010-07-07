#ifndef PAXOSLEASELEARNERSTATE_H
#define PAXOSLEASELEARNERSTATE_H

#include "System/Platform.h"

/*
===============================================================================

 PaxosLeaseLearnerState

===============================================================================
*/

struct PaxosLeaseLearnerState
{
	void Init()
	{
		learned		= 0;
		leaseOwner	= 0;
		expireTime	= 0;
		leaseEpoch	= 0;
	}
	
	void OnLeaseTimeout()
	{
		learned		= 0;
		leaseOwner	= 0;
		expireTime	= 0;
		leaseEpoch++;
	}
	
	bool		learned;
	unsigned	leaseOwner;
	uint64_t	expireTime;
	uint64_t	leaseEpoch;
};

#endif
