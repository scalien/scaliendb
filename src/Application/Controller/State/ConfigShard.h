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
	
	ConfigShard*	prev;
	ConfigShard*	next;
};

#endif
