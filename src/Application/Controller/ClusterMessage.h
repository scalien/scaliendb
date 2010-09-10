#ifndef CLUSTERMESSAGE_H
#define CLUSTERMESSAGE_H

#include "System/Common.h"
#include "Framework/Messaging/Message.h"

#define			CLUSTER_SET_NODEID		'N'

class ClusterMessage : public Message
{
public:
	char		type;
	uint64_t	nodeID;
	
	void		SetNodeID(uint64_t nodeID);

	bool		Read(ReadBuffer& buffer);
	bool		Write(Buffer& buffer);
};

#endif
