#ifndef CONFIGMESSAGE_H
#define CONFIGMESSAGE_H

#include "System/Platform.h"
#include "System/IO/Endpoint.h"
#include "System/Buffers/ReadBuffer.h"

#define CONFIG_CREATE_CHUNK	'C'

class ConfigMessage
{
public:
	char			type;

	uint64_t		chunkID;
	
	unsigned		numNodes;
	uint64_t		nodeIDs[7];
	Endpoint		endpoints[7];

	bool			Read(const ReadBuffer& buffer);
	bool			Write(Buffer& buffer);
};

#endif
