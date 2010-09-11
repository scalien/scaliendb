#ifndef CONFIGTABLE_H
#define CONFIGTABLE_H

#include "System/Common.h"
#include "System/Containers/InList.h"
#include "System/Buffers/Buffer.h"
#include "ConfigShard.h"

/*
===============================================================================

 ConfigTable

===============================================================================
*/

class ConfigTable
{
public:
	typedef InList<ConfigShard> ShardList;
	
	uint64_t		tableID;
	//uint64_t		databaseID;
	Buffer			name;
	
	ShardList		shards;
	
	ConfigTable*	prev;
	ConfigTable*	next;
};

#endif
