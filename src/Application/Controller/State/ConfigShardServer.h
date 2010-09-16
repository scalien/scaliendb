#ifndef CONFIGSHARDSERVER_H
#define CONFIGSHARDSERVER_H

#include "System/Common.h"
#include "System/Containers/List.h"
#include "System/IO/Endpoint.h"

/*
===============================================================================================

 ConfigShardServer

===============================================================================================
*/

class ConfigShardServer
{
public:
	ConfigShardServer()	{ prev = next = this; }

	uint64_t			nodeID;
	Endpoint			endpoint;
	
	ConfigShardServer*	prev;
	ConfigShardServer*	next;
};

#endif
