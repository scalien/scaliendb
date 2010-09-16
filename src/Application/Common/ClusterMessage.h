#ifndef CLUSTERMESSAGE_H
#define CLUSTERMESSAGE_H

#include "System/Common.h"
#include "Framework/Messaging/Message.h"

#define			CLUSTER_SET_NODEID			'N'
#define			CLUSTER_PRIMARY_LEASE		'L'
#define			CLUSTER_CONFIG_INFO			'C'

/*
===============================================================================================

 ClusterMessage

===============================================================================================
*/

class ClusterMessage : public Message
{
public:
	char		type;
	uint64_t	nodeID;
	uint64_t	shardID;
	unsigned	leaseTime;
	
	void		SetNodeID(uint64_t nodeID);
	
	bool		Read(ReadBuffer& buffer);
	bool		Write(Buffer& buffer);
};

#endif
