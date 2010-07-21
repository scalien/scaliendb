#ifndef PAXOSACCEPTORSTATE_H
#define PAXOSACCEPTORSTATE_H

#include <stdint.h>
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
	Buffer			acceptedValue;

};

/*
===============================================================================
*/

inline void PaxosAcceptorState::Init()
{
	promisedProposalID = 0;
	accepted = false;
	acceptedProposalID = 0;
	acceptedValue.Rewind();
}

#endif
