#ifndef CLUSTERCONTEXT_H
#define CLUSTERCONTEXT_H

#include "System/Buffers/ReadBuffer.h"
#include "System/IO/Endpoint.h"

/*
===============================================================================

 ClusterContext

===============================================================================
*/

class ClusterContext
{
public:
	~ClusterContext() {}
	
	virtual void OnClusterMessage(ReadBuffer& msg)								= 0;
	virtual void OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint)	= 0;
};

#endif
