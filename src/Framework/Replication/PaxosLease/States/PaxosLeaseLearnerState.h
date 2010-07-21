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
	void			Init();
	void			OnLeaseTimeout();
	
	bool			learned;
	unsigned		leaseOwner;
	uint64_t		expireTime;
	uint64_t		leaseEpoch;
};

/*
===============================================================================
*/

inline void PaxosLeaseLearnerState::Init()
{
	learned		= 0;
	leaseOwner	= 0;
	expireTime	= 0;
	leaseEpoch	= 0;
}

inline void PaxosLeaseLearnerState::OnLeaseTimeout()
{
	learned		= 0;
	leaseOwner	= 0;
	expireTime	= 0;
	leaseEpoch++;
}

#endif
