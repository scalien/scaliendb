#ifndef CLUSTERSTATE_H
#define CLUSTERSTATE_H

#include "Application/Common/ClusterMessage.h"

class ClusterState
{
public:
	void	Update(uint64_t nodeID, ClusterMessage& command);
	
	// for each table, we have a replication target
	// for each table, we have at least one shard
	//
	// for each shard, we have a versionID
	// for each shard, we have a quorum consisting of shard servers
	// for each shard, we either have a primary or not
};

#endif
