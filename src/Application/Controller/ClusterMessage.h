#ifndef CLUSTERMESSAGE_H
#define CLUSTERMESSAGE_H

#include "Framework/Messaging/Message.h"
#include "System/IO/Endpoint.h"
#include "System/Buffers/ReadBuffer.h"

#define CLUSTER_PROTOCOL_ID		'C'

#define CLUSTER_INFO			'I'

class ClusterMessage : public Message
{
public:
	char			type;

	uint64_t		chunkID;
	
	unsigned		numNodes;
	uint64_t		nodeIDs[7];
	Endpoint		endpoints[7];

	bool			Read(ReadBuffer& buffer);
	bool			Write(Buffer& buffer);
};

#endif
