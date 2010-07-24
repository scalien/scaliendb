#ifndef PAXOSACCEPTORSTATE_H
#define PAXOSACCEPTORSTATE_H

#include "System/Platform.h"
#include "System/Buffers/Buffer.h"

/*
===============================================================================

 PaxosAcceptorState

===============================================================================
*/

struct PaxosAcceptorState
{
	void			Init();

	uint64_t		promisedProposalID;
	bool			accepted;	
	uint64_t		acceptedProposalID;
	uint64_t		acceptedRunID;
	uint64_t		acceptedEpochID;
	Buffer			acceptedValue;

};

/*
===============================================================================
*/

//inline void PaxosAcceptorState::Init()
//{
//	promisedProposalID = 0;
//	accepted = false;
//	acceptedProposalID = 0;
//	acceptedValue.Clear();
//}

#endif
