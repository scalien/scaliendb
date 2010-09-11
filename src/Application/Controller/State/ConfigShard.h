#ifndef CONFIGSHARD_H
#define CONFIGSHARD_H

#include "System/Common.h"
#include "System/Containers/List.h"

/*
===============================================================================

 ConfigShard

===============================================================================
*/

class ConfigShard
{
public:
	uint64_t		shardID;
	unsigned		replicationTarget;
	
	List<uint64_t>	quorum;
	bool			hasPrimary;	// this is not replicated, only stored by the MASTER in-memory
	uint64_t		primaryID;	// this is not replicated, only stored by the MASTER in-memory

	ConfigShard*	prev;
	ConfigShard*	next;
};

#endif
