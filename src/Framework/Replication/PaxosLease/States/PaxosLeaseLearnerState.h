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
	uint64_t		leaseOwner;
	uint64_t		expireTime;
};

/*
===============================================================================
*/

inline void PaxosLeaseLearnerState::Init()
{
	learned		= false;
	leaseOwner	= 0;
	expireTime	= 0;
}

inline void PaxosLeaseLearnerState::OnLeaseTimeout()
{
	learned		= false;
	leaseOwner	= 0;
	expireTime	= 0;
}

#endif
