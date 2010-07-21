#ifndef PAXOSLEARNERSTATE_H
#define PAXOSLEARNERSTATE_H

#include "System/Buffers/Buffer.h"

/*
===============================================================================

 PaxosLearnerState

===============================================================================
*/

struct PaxosLearnerState
{
	void			Init();

	bool			learned;
	Buffer			value;
};

/*
===============================================================================
*/

inline void PaxosLearnerState::Init()
{
	learned = 0;
	value.Rewind();
}

#endif
